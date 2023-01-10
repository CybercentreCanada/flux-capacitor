package cccs.fluxcapacitor.tag

import org.apache.log4j.Logger

import com.google.common.hash.BloomFilter
import cccs.fluxcapacitor.RuleConf
import cccs.fluxcapacitor.TagCache

class OrderedTag(
    tagName: String,
    ruleConf: RuleConf,
    tagCache: TagCache
) extends DefaultTag(tagName, ruleConf, tagCache) {

  val putCondition = {
    val i = ruleConf.tags.indexWhere(tag => tag.name == tagName) - 1
    if (i >= 0) {
      ruleConf.tags(i).name
    } else {
      ""
    }
  }

  var prerequisiteTag: Option[Tag] = None

  def getPutCondition(): String = putCondition

  def setPrerequisiteTag(tag: Option[Tag]) = prerequisiteTag = tag

  def getPrerequisiteTag() = prerequisiteTag.get

  override def evaluate() = {
    if (log.isTraceEnabled) log.trace("evaluating OrderedTag")

    if (putCondition.isEmpty()) {
      if (log.isTraceEnabled)
        log.trace("no put conditions, invoking base class to handle it")
      super.evaluate()
    } else {
      var preTag = getPrerequisiteTag().asInstanceOf[CachableTag]
      // we will refer to the tag specified in the put_condition as "that" tag
      if (log.isTraceEnabled) log.trace("ordered tag dependes on : " + preTag)
      // Tag thatTag = this.tagMap.get(thatTagName)
      // assert (thatTag instanceof BaseTag)
      // merge retrieved value with the current value of "that" tag
      // note "that" tag has already been evaluated (the tags are sorted apriori)
      if (log.isTraceEnabled) log.trace("prerequisiteTag is cached: " + preTag)
      if (preTag.isCached()) {
        // previous event has been seen, we can proceed.
        pushOrPullFromCache()
      }
    }
    currentValue()
  }
}
