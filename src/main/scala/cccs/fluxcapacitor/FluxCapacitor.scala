package cccs.fluxcapacitor

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.streaming.OutputMode.Append

import java.sql.Timestamp
import java.nio.ByteBuffer
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import com.esotericsoftware.kryo.Kryo
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import scala.collection.mutable.ArrayBuffer

case class FluxState(
    var version: Int,
    var active: Int,
    var serializedBlooms: List[Array[Byte]]
) extends TagCache {

  val log = Logger.getLogger(this.getClass)

  @transient var blooms: List[BloomFilter[CharSequence]] = List()
  @transient var putCount = 0L
  @transient val desiredFpp = 0.0001
  @transient val NUM_BLOOMS = 10
  @transient var tagCapacity = 0

  override def put(tagName: String): Unit = {
    putCount += 1L
    getActiveBloom().put(tagName)
  }

  override def get(tagName: String): Boolean = {
    getActiveBloom().mightContain(tagName)
  }

  def init(): FluxState = {
    val bloom = createBloom()
    blooms = List(bloom)
    this
  }

  def toState(): FluxState = {
    version += 1
    // we only store the active blooms, all other blooms were un-changed
    // we make sure to pre-allocate the output buffer, this has drastic measured performance improvement
    // in an experiment each micro-batch took 80 seconds, with pre-allocated buffer it went down to 30 seconds.
    // Doing this also help JVM GC pressure, there is way less intermediary arrays created while it grows.
    // We know approximately the size of the bloom in bytes so we can easily pre-allocate the buffer.
    // Note the size depends on the false positive 
    val approxsize = tagCapacity / NUM_BLOOMS * 3
    val byteArrayOut = new ByteArrayOutputStream(approxsize)
    val store = new ObjectOutputStream(byteArrayOut)
    store.writeObject(getActiveBloom())
    store.close
    serializedBlooms = List(byteArrayOut.toByteArray())
    val fpp = getActiveBloom().expectedFpp()
    log.debug(
      f"preparing for storage, active bloom is $active and it's fpp is $fpp%1.8f, num puts: $putCount"
    )
    this
  }

  def getActiveBloom(): BloomFilter[CharSequence] = {
    blooms(active)
  }

  def cycleBloom(): Unit = {
    log.debug("cycling blooms")
    active = (active + 1) % NUM_BLOOMS
    val newBloom = createBloom()
    if (blooms.length == NUM_BLOOMS) {
      log.debug(s"replacing bloom at index $active")
      val buff = blooms.to[ArrayBuffer]
      buff(active) = newBloom
      blooms = buff.toList
    } else {
      log.debug("adding a bloom to the list")
      blooms = blooms ++ List(newBloom)
    }
    val fpp = getActiveBloom().expectedFpp()
    log.debug(f"active bloom is $active and it's fpp is $fpp%1.8f")
  }

  def fromState(): FluxState = {
    blooms = serializedBlooms
      .map(theBytes => {
        val byteArrayInput = new ByteArrayInputStream(theBytes)
        val input = new ObjectInputStream(byteArrayInput)
        val obj = input.readObject()
        obj.asInstanceOf[BloomFilter[CharSequence]]
      })

    val fpp = getActiveBloom().expectedFpp()
    if (fpp > desiredFpp) {
      log.debug(f"active bloom $active is full. fpp $fpp%1.8f > $desiredFpp")
      cycleBloom()
    }
    else if (version % 5 == 0) {
      log.debug(f"trigger bloom cycle, just for testing!")
      cycleBloom()
    }

    this
  }

  def createBloom() = {
    val bloomCapacity: Int = tagCapacity / NUM_BLOOMS
    val bloom = BloomFilter.create(Funnels.stringFunnel(), bloomCapacity, desiredFpp)
    //val prep = (bloomCapacity * 0.95).toInt
    //(1 to prep).map("padding" + _).foreach(s => bloom.put(s))
    bloom
  }

}
case class FluxCapacitorMapFunction(
    val tagCapacity: Int,
    val specification: String
) extends Serializable {
  @transient lazy val log = Logger.getLogger(this.getClass)

  def sortInputRows(orderedRows: Seq[Row]) = {
    var timestampIndex = orderedRows.iterator.next().fieldIndex("timestamp")
    var startSort = System.currentTimeMillis()
    val sorted = orderedRows.sortWith((r1: Row, r2: Row) =>
      r1.getTimestamp(timestampIndex)
        .compareTo(r2.getTimestamp(timestampIndex)) <= 0
    )
    var endSort = System.currentTimeMillis()
    log.debug(
      "time to sort " + sorted.size + " input rows (ms): " + (endSort - startSort)
    )
    sorted
  }

  def checkSorted(rows: Seq[Row]): Unit = {
    var timestampIndex = rows.head.fieldIndex("timestamp")
    var prev = new Timestamp(0)
    val it = rows.iterator
    while (it.hasNext) {
      var row = it.next
      var ts = row.getTimestamp(timestampIndex)
      if (prev == None) {
        prev = ts
      } else if (prev.compareTo(ts) <= 0) {
        prev = ts
      } else {
        log.error("!!!prev row " + prev + " preceeds current row " + ts)
        prev = ts
      }
    }
  }

  def processBatch(
      key: String,
      rows: Iterator[Row],
      state: GroupState[FluxState]
  ): Iterator[Row] = {

    val startTotal = System.currentTimeMillis()
    var fluxState: FluxState =
      state.exists match {
        case true => {
          val fluxState = state.get
          fluxState.tagCapacity = tagCapacity
          fluxState.fromState()
        }
        case false => {
          val fluxState = FluxState(0, 0, List())
          fluxState.tagCapacity = tagCapacity
          fluxState.init()
        }
      }

    val sorted = sortInputRows(rows.toSeq)
    checkSorted(sorted)
    // val sorted = rows.toSeq
    val rulesConf = RulesConf.load(specification)

    val rules = new RulesAdapter(new Rules(rulesConf, fluxState))
    val outputRows = sorted.map(row => rules.evaluateRow(row))

    state.update(fluxState.toState())
    log.debug(outputRows.size + " rows processed for group key: " + key)
    val endTotal = System.currentTimeMillis
    log.debug(
      "total time spent in map with group state (ms): " + (endTotal - startTotal)
    )

    outputRows.iterator
  }

}

object FluxCapacitor {

  def invoke(
      df: Dataset[Row],
      groupKey: String,
      tagCapacity: Int,
      specification: String
  ): Dataset[Row] = {
    val sparkSession: SparkSession = SparkSession.builder.getOrCreate
    import sparkSession.implicits._
    val outputEncoder = RowEncoder(df.schema).resolveAndBind()

    val stateEncoder = Encoders.product[FluxState]
    val func = new FluxCapacitorMapFunction(tagCapacity, specification)

    var groupKeyIndex = df.schema.fieldIndex(groupKey)

    df
      .groupByKey(row => row.getString(groupKeyIndex))
      .flatMapGroupsWithState(
        Append,
        GroupStateTimeout.NoTimeout()
      )(func.processBatch)(stateEncoder, outputEncoder)
  }
}
