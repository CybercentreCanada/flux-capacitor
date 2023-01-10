package cccs.fluxcapacitor.tag

import cccs.fluxcapacitor.RuleConf
import com.google.common.hash.BloomFilter
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import cccs.fluxcapacitor.TagCache

abstract class Tag {
  val log = Logger.getLogger(this.getClass)

  def currentValue(): Boolean

  def evaluateRow(row: Row): Boolean

  def setCurrentValue(value: Boolean): Tag
}

object Tag {
  val log = Logger.getLogger(this.getClass)

  def createTag(ruleConf: RuleConf, name: String, tagCache: TagCache): Tag = {
    if (log.isTraceEnabled) log.trace(s"creating : ${ruleConf.rulename} tagname: $name")

    ruleConf.action match {
      case "parent" => {
        if (log.isTraceEnabled) log.trace("creating a parent tag for parent action")
        new ParentTag(name, ruleConf, tagCache)
      }
      case "ancestor" => {
        if (log.isTraceEnabled) log.trace("creating a ancestor tag for ancestor action")
        new AncestorTag(name, ruleConf, tagCache)
      }
      case "temporal" if ruleConf.ordered => {
        if (log.isTraceEnabled) log.trace("creating a ordered tag for ordered action")
        new OrderedTag(name, ruleConf, tagCache)
      }
      case "temporal" if ruleConf.ordered == false => {
        if (log.isTraceEnabled) log.trace("creating a default tag for un-ordered action")
        new DefaultTag(name, ruleConf, tagCache)
      }
      case _ =>
        throw new RuntimeException("rule action unsupported " + ruleConf.action)
    }
  }
}
