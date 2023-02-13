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
import cccs.fluxcapacitor.tag.DefaultTag

class Rule(ruleConf: RuleConf, tagCache: TagCache) {
  val log = Logger.getLogger(this.getClass)

  val tagEvaluatorMap: Map[String, Tag] = ruleConf.tags
    .map(tag => (tag.name -> Tag.createTag(ruleConf, tag.name, tagCache)))
    .toMap

  def getEvaluator(name: String): Tag = {
    tagEvaluatorMap.get(name) match {
      case Some(tag) => tag
      case None      => new TransitoryTag()
    }
  }

  var orderedInitedEvaluators:Option[Seq[(String, Tag)]] = None
  
  def getEvaluators(sigmaResults: SigmaMap) = {
    if (orderedInitedEvaluators == None) {
      val tagValuesMap = sigmaResults.getOrElse(ruleConf.rulename, Map.empty)
      val allTagNames = tagEvaluatorMap.keySet ++ tagValuesMap.keySet
      val allTagEvaluatorMap = allTagNames
      // for every tag, create a tag evaluator
      .map(tagName => (tagName, getEvaluator(tagName)))
    

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
      orderedInitedEvaluators = Some(orderedEvaluators
        .map({ case (tagName, evaluator) =>
          if (evaluator.isInstanceOf[OrderedTag]) {
            val orderedTag = evaluator.asInstanceOf[OrderedTag]
            val prerequisite = orderedTag.getPutCondition()
            val prerequisiteTag = tagEvaluatorMap.get(prerequisite)
            orderedTag.setPrerequisiteTag(prerequisiteTag)
          }
          (tagName -> evaluator)
        }))
    }
    orderedInitedEvaluators.get
  }

  def evaluateRow(row: Row, sigmaResults: SigmaMap): TagMap = {
    if (log.isTraceEnabled) log.trace(s"evaluating rule in this order ${ruleConf.rulename}")
    val tagValuesMap = sigmaResults.getOrElse(ruleConf.rulename, Map.empty)
    var shouldEval = true
    if (ruleConf.action == "temporal") {
      val allTemporalAreFalse = getEvaluators(sigmaResults)
        .map({ case (tagName, evaluator) =>
          if (evaluator.isInstanceOf[TransitoryTag]) {
            false
          }
          else {
            tagValuesMap.getOrElse(tagName, false)
          }
        })
        .forall(_ == false)
      shouldEval = !allTemporalAreFalse
    }

    if (shouldEval) {
      getEvaluators(sigmaResults)
        .map({ case (tagName, evaluator) =>
          if (log.isTraceEnabled) log.trace(s"Evaluating tag: $tagName")
          val value = tagValuesMap.getOrElse(tagName, false)
          evaluator.setCurrentValue(value)
          var retValue = evaluator.evaluateRow(row)
          if (log.isTraceEnabled) log.trace(s"Tag: $tagName evaluated current value: $retValue")
          (tagName -> retValue)
        })
        .toMap
    }
    else {
        tagValuesMap
    }
  }
}
