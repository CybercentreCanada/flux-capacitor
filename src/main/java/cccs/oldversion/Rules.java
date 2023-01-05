package cccs.oldversion;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Row;

import com.google.common.hash.BloomFilter;

public class Rules {

  private List<Rule> ruleEvaluators;

  public Rules(RulesConf conf, BloomFilter<CharSequence> bloom) {
    this.ruleEvaluators = conf.rules.stream()
        .map(ruleConf -> new Rule(ruleConf, bloom))
        .collect(Collectors.toList());
  }

  public Rules(RulesConf conf) {
    this(conf, null);
  }

  public Map<String, Map<String, Boolean>> evaluateRow(Row row, Map<String, Map<String, Boolean>> results) {
    return ruleEvaluators.stream()
        .map(rule -> ImmutablePair.of(rule.getRuleName(), rule.evaluateRow(row, results)))
        .collect(Collectors.toMap(p -> p.left, p -> p.right));
  }
}