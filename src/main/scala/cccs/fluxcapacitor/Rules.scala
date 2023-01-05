package cccs.fluxcapacitor

import com.google.common.hash.BloomFilter
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.spark.sql.Row

import java.util.List
import java.util.stream.Collectors

import cccs.fluxcapacitor.RulesConf.SigmaMap

class Rules(conf: RulesConf, tagCache: TagCache) {
  var ruleEvaluators = conf.rules
    .map(conf => (conf.rulename -> new Rule(conf, tagCache)))
    .toMap

  def evaluateRow(row: Row, results: SigmaMap): SigmaMap = {
    ruleEvaluators.map({ case (ruleName, rule) =>
      (ruleName -> rule.evaluateRow(row, results))
    })
  }
}
