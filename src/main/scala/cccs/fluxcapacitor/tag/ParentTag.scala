package cccs.fluxcapacitor.tag

import org.apache.log4j.Logger

import com.google.common.hash.BloomFilter

import cccs.fluxcapacitor.RuleConf
import cccs.fluxcapacitor.TagCache

class ParentTag(tagName: String, ruleConf: RuleConf, tagCache: TagCache)
    extends CachableTag(tagName, ruleConf, tagCache) {

  override protected def makeBloomPutKey() =
    makeBloomKey(ruleConf.child)

  override protected def makeBloomGetKey() =
    makeBloomKey(ruleConf.parent)

  override def evaluate() = {
    if (log.isTraceEnabled) log.trace("evaluating")
    pushOrPullFromCache()
    currentValue()
  }
}
