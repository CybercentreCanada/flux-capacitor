package cccs.fluxcapacitor.tag

import org.apache.spark.sql.Row

class TransitoryTag(val value: Boolean) extends Tag {

  override def currentValue(): Boolean = value

  override def evaluateRow(row: Row) = {
    if (log.isTraceEnabled) log.trace("evaluating ImmutableTag")
    currentValue()
  }

  override def setCurrentValue(value: Boolean): Tag = this // noop
}

object TransitoryTag {
  val trueTag = new TransitoryTag(true)
  val faslseTag = new TransitoryTag(false)

  def of(value: Boolean) = value match {
    case true  => trueTag
    case false => faslseTag
  }
}
