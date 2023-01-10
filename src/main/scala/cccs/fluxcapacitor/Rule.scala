package cccs.fluxcapacitor
import cccs.fluxcapacitor.tag.CachableTag
import cccs.fluxcapacitor.tag.OrderedTag
import cccs.fluxcapacitor.tag.Tag
import cccs.fluxcapacitor.tag.TransitoryTag
import com.google.common.hash.BloomFilter
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import scala.collection.Map
import cccs.fluxcapacitor.RulesConf.TagMap
import cccs.fluxcapacitor.RulesConf.SigmaMap

class Rule(ruleConf: RuleConf, tagCache: TagCache) {
  val log = Logger.getLogger(this.getClass)

  val tagEvaluatorMap: Map[String, Tag] = ruleConf.tags
    .map(tag => (tag.name -> Tag.createTag(ruleConf, tag.name, tagCache)))
    .toMap

  def getEvaluator(name: String, currentValue: Boolean): Tag = {
    tagEvaluatorMap.get(name) match {
      case Some(tag) => tag.setCurrentValue(currentValue)
      case None      => TransitoryTag.of(currentValue)
    }
  }

  def evaluateRow(row: Row, sigmaResults: SigmaMap): TagMap = {
    if (log.isTraceEnabled) log.trace(s"evaluating rule in this order ${ruleConf.rulename}")
    val tagValuesMap = sigmaResults.getOrElse(ruleConf.rulename, Map.empty)
    val allTagNames = tagEvaluatorMap.keySet ++ tagValuesMap.keySet
    val allTagEvaluatorMap = allTagNames
      // for every tag, get the value or set it to false
      .map(tagName => (tagName -> tagValuesMap.getOrElse(tagName, false)))
      // for every tag, create a tag evaluator
      .map({ case (tagName, value) => (tagName, getEvaluator(tagName, value)) })

    // order evaulators according to inter dependencies
    val orderedEvaluators = allTagEvaluatorMap.toSeq
      .sortWith({ case ((tag1, evaluator1), (tag2, evaluator2)) =>
        if (evaluator1.isInstanceOf[OrderedTag]) {
          val e1OrderedTag = evaluator1.asInstanceOf[OrderedTag]
          if (tag2.equals(e1OrderedTag.getPutCondition())) {
            // e1 depends on e2
            true
          }
        }
        false
      })
    if (log.isTraceEnabled) log.trace("tag evaluation order is as follows: " + orderedEvaluators)

    // find and set pre-requisit evaluators
    val orderedInitedEvaluators = orderedEvaluators
      .map({ case (tagName, evaluator) =>
        if (evaluator.isInstanceOf[OrderedTag]) {
          val orderedTag = evaluator.asInstanceOf[OrderedTag]
          val prerequisite = orderedTag.getPutCondition()
          val prerequisiteTag = tagEvaluatorMap.get(prerequisite)
          orderedTag.setPrerequisiteTag(prerequisiteTag)
        }
        (tagName -> evaluator)
      })

    orderedInitedEvaluators
      .map({ case (tagName, evaluator) =>
        if (log.isTraceEnabled) log.trace(s"Evaluating tag: $tagName")
        var value = evaluator.evaluateRow(row)
        if (log.isTraceEnabled) log.trace(s"Tag: $tagName evaluated current value: $value")
        (tagName -> value)
      })
      .toMap
  }
}
