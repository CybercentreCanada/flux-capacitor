package cccs.oldversion.tag;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;

public class DefaultTag extends CachableTag {
    static Logger log = Logger.getLogger(DefaultTag.class.getName());

    public DefaultTag(String tagname, RuleConf ruleConf, BloomFilter<CharSequence> bloom) {
        super(tagname, ruleConf, bloom);
    }

    @Override
    public boolean evaluate() {
        log.debug("evaluating DefaultTag");
        pushOrPullFromCache();
        return currentValue();
    }

    @Override
    protected String makeBloomPutKey() {
        String prefix = "";
        if (ruleConf.groupby != null) {
            prefix = ruleConf.groupby.get(0);
        }
        return makeBloomKey(prefix, ruleConf.rulename, tagname, row);
    }

    @Override
    protected String makeBloomGetKey() {
        String prefix = "";
        if (ruleConf.groupby != null) {
            prefix = ruleConf.groupby.get(0);
        }
        return makeBloomKey(prefix, ruleConf.rulename, tagname, row);
    }
}