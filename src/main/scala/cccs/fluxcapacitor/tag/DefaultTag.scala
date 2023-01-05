package cccs.fluxcapacitor.tag

import org.apache.log4j.Logger

import com.google.common.hash.BloomFilter

import cccs.fluxcapacitor.RuleConf
import cccs.fluxcapacitor.TagCache

class DefaultTag(
    tagName: String,
    ruleConf: RuleConf,
    tagCache: TagCache
) extends CachableTag(tagName, ruleConf, tagCache) {

  override def evaluate(): Boolean = {
    log.debug("evaluating DefaultTag")
    pushOrPullFromCache()
    currentValue()
  }

  // by default uses the same get/put key values
  def makeBloomPutGetKey(): String = {
    if (ruleConf.groupby != null) {
      // if a groupby is specified
      val colname = ruleConf.groupby.head
      makeBloomKey(ruleConf.rulename, tagName, colname)
    } else {
      makeBloomKey(ruleConf.rulename, tagName)
    }
  }

  override protected def makeBloomPutKey() = makeBloomPutGetKey

  override protected def makeBloomGetKey() = makeBloomPutGetKey
}
