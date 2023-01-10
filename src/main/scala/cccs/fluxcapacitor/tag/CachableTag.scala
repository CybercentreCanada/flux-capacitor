package cccs.fluxcapacitor.tag

import cccs.fluxcapacitor.RuleConf
import com.google.common.hash.BloomFilter
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import cccs.fluxcapacitor.TagCache

abstract class CachableTag(
    val tagName: String,
    val ruleConf: RuleConf,
    val tagCache: TagCache
) extends Tag {

  protected def evaluate(): Boolean

  protected def makeBloomPutKey(): String

  protected def makeBloomGetKey(): String

  val baseBloomKey = ruleConf.rulename + "." + tagName
  var bloomGetKey: String = ""
  var bloomPutKey: String = ""
  var value: Boolean = false
  var row: Option[Row] = None
  var evaluated: Boolean = false

  override def toString(): String = {
    if (evaluated) {
      s"(tagName: $tagName, bloomGetKey: $bloomGetKey, bloomPutKey: $bloomPutKey, value: $value, isCached: ${isCached()})"
    } else {
      s"(tagName: $tagName)"
    }
  }

  override def evaluateRow(row: Row): Boolean = {
    this.row = Option(row)
    bloomGetKey = makeBloomGetKey()
    bloomPutKey = makeBloomPutKey()
    evaluated = true
    evaluate()
  }

  private def getColumnValue(colname: String): String = {
    if (colname.isEmpty()) {
      ""
    } else {
      var i = row.get.fieldIndex(colname)
      if (row.get.isNullAt(i)) {
        ""
      } else {
        row.get.getString(i)
      }
    }
  }

  protected def makeBloomKey(colname: String = ""): String = {
    if (colname.isEmpty()) {
      baseBloomKey
    } else {
      var suffix = getColumnValue(colname)
      if (log.isTraceEnabled)
        log.trace(s"suffix [$suffix] retrieved for column [$colname]")
      baseBloomKey + "." + suffix
    }
  }

  def storeInCache(): Unit = {
    if(log.isTraceEnabled()) log.trace(s"storing key: $bloomPutKey in bloom")
    tagCache.put(bloomPutKey)
  }

  def isCached(): Boolean = tagCache.get(bloomGetKey)

  override def setCurrentValue(newValue: Boolean): Tag = {
    bloomGetKey = ""
    bloomPutKey = ""
    evaluated = false
    this.value = newValue
    this
  }

  def currentValue(): Boolean = value

  protected def mergeCachedWithCurrent() = {
    this.value = currentValue() | isCached()
  }

  protected def pushOrPullFromCache() = {
    if (currentValue()) {
      // put "this" tag in the bloom
      if (log.isTraceEnabled())
        log.trace("the tag was raised in this row, store tag in bloom")
      storeInCache()
    } else {
      // get "this" tag from the bloom
      if (log.isTraceEnabled())
        log.trace("current value is false, cached value is: " + isCached())
      mergeCachedWithCurrent()
      if (log.isTraceEnabled()) log.trace("merged value is " + currentValue())
    }
  }

}
