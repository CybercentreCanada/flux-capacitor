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

import java.sql.Timestamp

case class FluxState(bloomCapacity: Int) {
  val bloom = BloomFilter.create(Funnels.stringFunnel(), bloomCapacity, 0.01)
}

case class FluxCapacitorMapFunction(
    val bloomCapacity: Int,
    val specification: String
) extends Serializable {
  @transient lazy val log = Logger.getLogger(this.getClass)

  def sortInputRows(orderedRows: Seq[Row]) = {
    var timestampIndex = orderedRows.iterator.next().fieldIndex("timestamp")
    var startSort = System.currentTimeMillis()
    val sorted = orderedRows.sortWith((r1: Row, r2: Row) =>
      r1.getTimestamp(timestampIndex)
        .compareTo(r2.getTimestamp(timestampIndex)) >= 0
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
      key: Int,
      rows: Iterator[Row],
      state: GroupState[FluxState]
  ): Iterator[Row] = {
    val startTotal = System.currentTimeMillis()
    val fluxState: FluxState = state.exists match {
      case true  => state.get
      case false => FluxState(bloomCapacity)
    }

    var feature_set = 0
    var feature_get = 0

    val sorted = sortInputRows(rows.toSeq)
    checkSorted(sorted)

    val rulesConf = RulesConf.load(specification)

    val tagCache = new BloomTagCache(fluxState.bloom)

    val outputRows = sorted.map(row =>
      new RulesAdapter(new Rules(rulesConf, tagCache)).evaluateRow(row)
    )

    val fpp = fluxState.bloom.expectedFpp
    state.update(fluxState)
    log.debug(outputRows.size + " rows processed for group key: " + key)
    log.debug("puts: " + feature_set + " gets: " + feature_get + " fpp: " + fpp)
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
      bloomCapacity: Int,
      specification: String
  ): Dataset[Row] = {
    val sparkSession: SparkSession = SparkSession.builder.getOrCreate
    import sparkSession.implicits._
    val outputEncoder = RowEncoder(df.schema).resolveAndBind()
    val stateEncoder = Encoders.product[FluxState]
    val func = new FluxCapacitorMapFunction(bloomCapacity, specification)

    var groupKeyIndex = df.schema.fieldIndex(groupKey)

    df
      .groupByKey(row => row.getInt(groupKeyIndex))
      .flatMapGroupsWithState(
        Update,
        GroupStateTimeout.NoTimeout()
      )(func.processBatch)(stateEncoder, outputEncoder)
  }
}
