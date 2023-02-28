package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.util
import java.util.Locale
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.google.common.io.ByteStreams
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import cccs.fluxcapacitor.FluxState


class FluxStateStoreProvider extends HDFSBackedStateStoreProvider {

  // CCCS: Class representing a row value, spark puts the state inside a row with a "groupState" column.
  case class FluxGroupState(groupState: FluxState)

  implicit val fluxGroupStateEnc = Encoders
    .product[FluxGroupState]
    .asInstanceOf[ExpressionEncoder[FluxGroupState]]
  implicit val fluxGroupStateSerializer = fluxGroupStateEnc.createSerializer()
  implicit val keyEncoder = ExpressionEncoder[String]
  implicit val keySerializer = keyEncoder.createSerializer()

  // CCCS: FluxState fields
  val VERSION_INDEX = 0
  val ACTIVE_INDEX = 1
  val SERIALIZED_BLOOMS_INDEX = 2
  val PUTCOUNT_INDEX = 3
  val GETCOUNT_INDEX = 4
  val SORT_TIMER_INDEX = 5
  val UPDATETAGCACHE_TIMER_INDEX = 6
  val FROMSTATE_TIMER_INDEX = 7
  val TOSTATE_TIMER_INDEX = 8
  val FLUXSTATE_NUM_FIELDS = 9

  // CCCS: We hard code the number of blooms
  val NUM_BLOOMS = 10

  def getFluxStateRow(groupStateRow: UnsafeRow) = groupStateRow.getStruct(0, FLUXSTATE_NUM_FIELDS)
  def getVersion(row: UnsafeRow) = getFluxStateRow(row).getInt(VERSION_INDEX)
  def getActive(row: UnsafeRow) = getFluxStateRow(row).getInt(ACTIVE_INDEX)
  def getBloomArray(row: UnsafeRow) = getFluxStateRow(row).getArray(SERIALIZED_BLOOMS_INDEX)

  def getActiveValue(row: UnsafeRow) = getFluxStateRow(row).getInt(ACTIVE_INDEX)
  def getGetTagCount(row: UnsafeRow) = getFluxStateRow(row).getLong(GETCOUNT_INDEX)
  def getPutTagCount(row: UnsafeRow) = getFluxStateRow(row).getLong(PUTCOUNT_INDEX)
  def getSortTime(row: UnsafeRow) = getFluxStateRow(row).getLong(SORT_TIMER_INDEX)
  def getUpdateTagCacheTime(row: UnsafeRow) = getFluxStateRow(row).getLong(UPDATETAGCACHE_TIMER_INDEX)
  def getFromStateTimer(row: UnsafeRow) = getFluxStateRow(row).getLong(FROMSTATE_TIMER_INDEX)
  def getToStateTimer(row: UnsafeRow) = getFluxStateRow(row).getLong(TOSTATE_TIMER_INDEX)

  // CCCS
  def makeRowKey(groupKey:String, bloomIndex:Int) = keySerializer(groupKey + bloomIndex).asInstanceOf[UnsafeRow]
  // CCCS
  def makeRowValue(fluxState:FluxState) = fluxGroupStateSerializer(FluxGroupState(fluxState)).asInstanceOf[UnsafeRow]

  // CCCS: deserialize a row representing a FluxState
  def deserializeFluxState(value: UnsafeRow): FluxState = {
    // The FluxState struct has 4 fields, get each of them
    var version = getVersion(value)
    var active = getActive(value)
    var unsafeBloomArray = getBloomArray(value)
    // When storing state we get only 1 bloom to store, get that serialized bloom
    var modifiedSerializedBloom = unsafeBloomArray.getBinary(0)
    // We can now create a row to store
    FluxState(version, active, List(modifiedSerializedBloom))
  }

  // CCCS: Coalesce the values we put into the store
  // We need to find which bloom is the active one, we do this based on the versions
  def coalesceFluxStates(fluxStates: Seq[FluxState], startTime: Long): FluxState = {
    val binBloomArray = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
    var latestActive = -1
    var latestVersion = -1L
    for(fluxState <- fluxStates) {
      binBloomArray += fluxState.serializedBlooms(0)
      if (fluxState.version > latestVersion) {
          latestVersion = fluxState.version
          latestActive = fluxState.active
      }
    }
    FluxState(latestVersion, latestActive, binBloomArray.toList, fromStateTimer = startTime)
  }


  /** Get the state store for making updates to create a new `version` of the store. */
  override def getStore(version: Long): StateStore = {
    val store = super.getStore(version)
    new FluxStateStore(store)
  }

  class FluxStateStore(val delegate: StateStore)
    extends StateStore {

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      require(value != null, "Cannot put a null value")

      putTagCount.add(getPutTagCount(value))
      getTagCount.add(getGetTagCount(value))
      sortTimer.add(getSortTime(value))
      updateTagCacheTimer.add(getUpdateTagCacheTime(value))
      toStateTimer.add(System.currentTimeMillis() - getToStateTimer(value))
      fromStateTimer.add(getFromStateTimer(value))

      // Create the key to store this bloom using the bloom id (active)
      val newRowKey = makeRowKey(key.getString(0), getActiveValue(value))
      // Store key/value as normal
      delegate.put(newRowKey, value)
    }

    override def get(key: UnsafeRow): UnsafeRow = {
      // CCCS: The list of blooms for this key are stored in separate entries in the store
      // we will find all of these entries and create a FluxGroupState
      val start = System.currentTimeMillis()
      val groupKey = key.getString(0)
      val fluxStates = Range(0, NUM_BLOOMS)
          .map(bloomIndex => delegate.get(makeRowKey(groupKey, bloomIndex)))
          .filter(_ != null)
          .map(value => deserializeFluxState(value))

      if(fluxStates.length > 0) {
        makeRowValue(coalesceFluxStates(fluxStates, start))
      }
      else {
        // else we found none of the blooms
        null
      }
    }
    override def id: StateStoreId = delegate.id

    override def version: Long = delegate.version

    override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = delegate.prefixScan(prefixKey)

    override def remove(key: UnsafeRow): Unit = delegate.remove(key)

    override def commit(): Long = delegate.commit()

    override def abort(): Unit = delegate.abort()

    override def iterator(): Iterator[UnsafeRowPair] = delegate.iterator()

    override def metrics: StateStoreMetrics = delegate.metrics

    override def hasCommitted: Boolean = delegate.hasCommitted
    }

  // CCCS: added a metric to count number of tags set into blooms
  // this can be viewed in the spark UI
  private val getTagCount: LongAdder = new LongAdder
  private val putTagCount: LongAdder = new LongAdder
  private val sortTimer: LongAdder = new LongAdder
  private val updateTagCacheTimer: LongAdder = new LongAdder
  private val fromStateTimer: LongAdder = new LongAdder
  private val toStateTimer: LongAdder = new LongAdder

  private lazy val metricGetTagCount: StateStoreCustomSumMetric =
    StateStoreCustomSumMetric("getTagCount",
      "number of get operation on tag cache")
  private lazy val metricPutTagCount: StateStoreCustomSumMetric =
    StateStoreCustomSumMetric("putTagCount",
      "number of put operation on tag cache")
  private lazy val metricSortTimer: StateStoreCustomTimingMetric =
    StateStoreCustomTimingMetric("sortTimer",
      "time spent sorting row batches")
  private lazy val metricUpdateTagCacheTimer: StateStoreCustomTimingMetric =
    StateStoreCustomTimingMetric("updateTagCacheTimer",
      "time spent updating tag cache")
  private lazy val metricFromStateTimer: StateStoreCustomTimingMetric =
    StateStoreCustomTimingMetric("fromStateTimer",
      "time spent deserializing")
  private lazy val metricToStateTimer: StateStoreCustomTimingMetric =
    StateStoreCustomTimingMetric("toStateTimer",
      "time spent serializing")

  override def getMetricsForProvider(): Map[String, Long] = synchronized {
    
    val metricsMap = super.getMetricsForProvider()

    val metrics = Map(metricGetTagCount.name -> getTagCount.sum(),
      metricPutTagCount.name -> putTagCount.sum(),
      metricSortTimer.name -> sortTimer.sum(),
      metricUpdateTagCacheTimer.name -> updateTagCacheTimer.sum(),
      metricFromStateTimer.name -> fromStateTimer.sum(),
      metricToStateTimer.name -> toStateTimer.sum(),
    )

    getTagCount.reset()
    putTagCount.reset()
    sortTimer.reset()
    updateTagCacheTimer.reset()
    fromStateTimer.reset()
    toStateTimer.reset()
    
    metricsMap ++ metrics
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    val parentMetrics = super.supportedCustomMetrics

    val ourMetrics = metricGetTagCount :: metricPutTagCount :: metricSortTimer :: metricUpdateTagCacheTimer ::
      metricFromStateTimer :: metricToStateTimer ::
      Nil
    parentMetrics ++ ourMetrics
  }

}