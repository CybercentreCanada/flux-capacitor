package cccs.oldversion;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;
import cccs.oldversion.tag.CachableTag;
import cccs.oldversion.tag.OrderedTag;
import cccs.oldversion.tag.Tag;
import cccs.oldversion.tag.TransitoryTag;

public class Rule {
  static Logger log = Logger.getLogger(Rule.class.getName());
  private final RuleConf ruleConf;
  private final BloomFilter<CharSequence> bloom;
  private Map<String, Tag> tagEvaluators;

  public Rule(RuleConf ruleConf, BloomFilter<CharSequence> bloom) {
    this.ruleConf = ruleConf;
    this.bloom = bloom;
    tagEvaluators = ruleConf.tags.stream()
      .map(tag -> ImmutablePair.of(tag.name, Tag.createTag( ruleConf, tag.name, bloom)))
      .collect(Collectors.toMap(p -> p.left, p -> p.right));
  }

  public String getRuleName() {
    return ruleConf.rulename;
  }

  public Tag getEvaluator(String name, Boolean currentValue){
    Tag tag = tagEvaluators.get(name);
    if(tag != null){
      tag.setCurrentValue(currentValue);
    }
    else {
      tag = TransitoryTag.of(currentValue);
    }
    return tag;
  }
  
  public Map<String, Boolean> evaluateRow(Row row, Map<String, Map<String, Boolean>> sigmaResults) {

    log.debug("evaluating rule " + ruleConf.rulename);

    Map<String, Boolean> tagValuesMap = sigmaResults.getOrDefault(ruleConf.rulename, Collections.emptyMap());

    List<String> tagNames = ruleConf.tags.stream().map(tag -> tag.name).collect(Collectors.toList());

    Map<String, Tag> tagMap = Stream.concat(
        tagNames.stream(),
        tagValuesMap.keySet().stream())
        .distinct()
        // for every tag, get the value or set it to false
        .map(tagName -> ImmutablePair.of(tagName, tagValuesMap.getOrDefault(tagName, false)))
        // for every tag, create a tag evaluator
        .map(p -> ImmutablePair.of(p.left, getEvaluator(p.left, p.right)))
        .collect(Collectors.toMap(p -> p.left, p -> p.right));

      return tagMap.entrySet().stream()
        .sorted((e1, e2) -> {
          if (e1.getValue() instanceof OrderedTag) {
            OrderedTag e1OrderedTag = (OrderedTag) e1.getValue();
            if (e2.getKey().equals(e1OrderedTag.getPutCondition())) {
              // e1 depends on e2
              return 1;
            }
          }
          return -1;
        })
        .map(e -> {
          if (e.getValue() instanceof OrderedTag) {
            OrderedTag orderedTag = (OrderedTag) e.getValue();
            String prerequisite = orderedTag.getPutCondition();
            Tag prerequisiteTag = tagMap.get(prerequisite);
            if (prerequisiteTag instanceof CachableTag) {
              // e1 depends on e2
              orderedTag.setPrerequisiteTag((CachableTag) prerequisiteTag);
            }
          }
          return e;
        })
        .map(e -> {
          String tagName = e.getKey();
          log.debug("Evaluating tag: " + tagName);
          boolean value = e.getValue().evaluateRow(row);
          log.debug("Tag: " + e.getKey() + " evaluated current value: " + value);
          return ImmutablePair.of(e.getKey(), value);
        })
        .collect(Collectors.toMap(p -> p.left, p -> p.right));
  }

}