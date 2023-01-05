package cccs.fluxcapacitor.tag

import cccs.fluxcapacitor.RuleConf
import com.google.common.hash.BloomFilter
import org.apache.log4j.Logger
import cccs.fluxcapacitor.TagCache

class AncestorTag(
    tagName: String,
    ruleConf: RuleConf,
    tagCache: TagCache
) extends ParentTag(tagName, ruleConf, tagCache) {

  override def evaluate() = {
    log.debug("evaluating AncestorTag")
    // get "that" tag from the bloom
    mergeCachedWithCurrent()
    if (currentValue()) {
      // put "this" tag in the bloom
      log.debug("the tag was raised in this row, store tag true in bloom")
      storeInCache()
    }
    currentValue()
  }
}
