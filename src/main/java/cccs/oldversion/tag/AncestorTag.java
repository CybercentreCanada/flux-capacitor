package cccs.oldversion.tag;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;

public class AncestorTag extends ParentTag {
    static Logger log = Logger.getLogger(AncestorTag.class.getName());

    public AncestorTag(String tagname, RuleConf ruleConf, BloomFilter<CharSequence> bloom) {
        super(tagname, ruleConf, bloom);
    }

    @Override
    public boolean evaluate() {
        log.debug("evaluating AncestorTag");
        // get "that" tag from the bloom
        mergeCachedWithCurrent();
        if (currentValue()) {
            // put "this" tag in the bloom
            log.debug("the tag was raised in this row, store tag true in bloom");
            storeInCache();
        }
        return currentValue();
    }
}