package cccs.fluxcapacitor.tag

import org.apache.spark.sql.Row

class TransitoryTag() extends Tag {

  var value = false

  override def currentValue(): Boolean = value

  override def evaluateRow(row: Row) = {
    if (log.isTraceEnabled) log.trace("evaluating ImmutableTag")
    currentValue()
  }

  override def setCurrentValue(value: Boolean): Tag = {
    this.value = value
    this
  }
}
