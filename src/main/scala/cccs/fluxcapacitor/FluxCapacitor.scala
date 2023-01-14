package cccs.fluxcapacitor

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.streaming.OutputMode.Append

import java.sql.Timestamp

case class FluxCapacitorMapFunction(
    val tagCapacity: Int,
    val specification: String,
    val useFluxStore: Boolean
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
          fluxState.useFluxStore = useFluxStore
          fluxState.fromState()
        }
        case false => {
          val fluxState = FluxState()
          fluxState.tagCapacity = tagCapacity
          fluxState.useFluxStore = useFluxStore
          fluxState.init()
        }
      }

    val startSort = System.currentTimeMillis()
    val sorted = sortInputRows(rows.toSeq)
    val endSort = System.currentTimeMillis()

    checkSorted(sorted)
    val startTagCacheUpdate = System.currentTimeMillis()
    val rulesConf = RulesConf.load(specification)
    val rules = new RulesAdapter(new Rules(rulesConf, fluxState))
    val outputRows = sorted.map(row => rules.evaluateRow(row))
    val endTagCacheUpdate = System.currentTimeMillis()

    fluxState.updateTagCacheTimer = endTagCacheUpdate - startTagCacheUpdate
    fluxState.sortTimer = endSort - startSort
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
      specification: String,
      useFluxStore: Boolean
  ): Dataset[Row] = {
    val sparkSession: SparkSession = SparkSession.builder.getOrCreate
    import sparkSession.implicits._
    val outputEncoder = RowEncoder(df.schema).resolveAndBind()

    val stateEncoder = Encoders.product[FluxState]
    val func = new FluxCapacitorMapFunction(tagCapacity, specification, useFluxStore)

    var groupKeyIndex = df.schema.fieldIndex(groupKey)

    df
      .groupByKey(row => row.getString(groupKeyIndex))
      .flatMapGroupsWithState(
        Append,
        GroupStateTimeout.NoTimeout()
      )(func.processBatch)(stateEncoder, outputEncoder)
  }
}
