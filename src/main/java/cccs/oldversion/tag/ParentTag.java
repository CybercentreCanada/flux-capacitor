package cccs.oldversion.tag;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;

public class ParentTag extends CachableTag {
    static Logger log = Logger.getLogger(ParentTag.class.getName());

    public ParentTag(String tagname, RuleConf ruleConf, BloomFilter<CharSequence> bloom) {
        super(tagname, ruleConf, bloom);
    }

    @Override
    protected String makeBloomPutKey() {
        return makeBloomKey(ruleConf.child, ruleConf.rulename, tagname, row);
    }

    @Override
    protected String makeBloomGetKey() {
        return makeBloomKey(ruleConf.parent, ruleConf.rulename, tagname, row);
    }

    @Override
    public boolean evaluate() {
        log.debug("evaluating");
        pushOrPullFromCache();
        return currentValue();
    }
}