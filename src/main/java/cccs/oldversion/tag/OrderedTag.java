package cccs.oldversion.tag;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;

public class OrderedTag extends DefaultTag {
    static Logger log = Logger.getLogger(OrderedTag.class.getName());

    private String putCondition;
    private CachableTag prerequisiteTag;

    public OrderedTag(String tagname, RuleConf ruleConf, BloomFilter<CharSequence> bloom) {
        super(tagname, ruleConf, bloom);
        String dependsOn = "";
        for (int i = 0; i < ruleConf.tags.size(); i++) {
            String tag = ruleConf.tags.get(i).name;
            if (tag == tagname) {
                break;
            }
            dependsOn = tag;
        }
        this.putCondition = dependsOn;
    }

    public String getPutCondition() {
        return this.putCondition;
    }

    public void setPrerequisiteTag(CachableTag prerequisiteTag) {
        this.prerequisiteTag = prerequisiteTag;
    }

    public CachableTag getPrerequisiteTag() {
        return this.prerequisiteTag;
    }

    @Override
    public boolean evaluate() {
        if (putCondition.isEmpty()) {
            return super.evaluate();
        }

        log.debug("evaluating OrderedTag");
        CachableTag prerequisiteTag = getPrerequisiteTag();
        // we will refer to the tag specified in the put_condition as "that" tag
        String thatTagName = getPutCondition();
        log.debug("ordered tag dependes on other tag: " + thatTagName);
        // Tag thatTag = this.tagMap.get(thatTagName);
        // assert (thatTag instanceof BaseTag);
        // merge retrieved value with the current value of "that" tag
        // note "that" tag has already been evaluated (the tags are sorted apriori)
        if (prerequisiteTag.isCached()) {
            // previous event has been seen, we can proceed.
            pushOrPullFromCache();
        }
        return currentValue();

    }
}