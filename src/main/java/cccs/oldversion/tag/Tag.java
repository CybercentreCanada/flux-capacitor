package cccs.oldversion.tag;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;

public abstract class Tag {
    static Logger log = Logger.getLogger(Tag.class.getName());

    public abstract boolean currentValue();

    public abstract boolean evaluateRow(Row row);

    public static Tag createTag(RuleConf ruleConf, String tagname,
            BloomFilter<CharSequence> bloom) {

        log.debug("creating tag object for rule: " + ruleConf.rulename + " tagname: " + tagname);
        if ("parent".equals(ruleConf.action)) {
            log.debug("creating a parent tag for parent action");
            return new ParentTag(tagname, ruleConf, bloom);
        }
        if ("ancestor".equals(ruleConf.action)) {
            log.debug("creating a ancestor tag for ancestor action");
            return new AncestorTag(tagname, ruleConf, bloom);
        } else if ("temporal".equals(ruleConf.action)) {
            if (ruleConf.ordered) {
                log.debug("creating a ordered tag for ordered action");
                return new OrderedTag(tagname, ruleConf, bloom);
            } else {
                log.debug("creating a default tag for un-ordered action");
                return new DefaultTag(tagname, ruleConf, bloom);
            }
        } else {
            throw new RuntimeException("rule action unsupported " + ruleConf.action);
        }
    }

    public abstract void setCurrentValue(boolean value);

}
