package cccs.fluxcapacitor

import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import java.util.stream.Collectors
import scala.collection.JavaConversions
import scala.collection.JavaConverters
import scala.collection.Map
import cccs.fluxcapacitor.RulesConf.SigmaMap

class RulesAdapter(val rules: Rules) {

  def evaluateRow(row: Row): Row = {
    val rIndex = row.fieldIndex("sigma")
    val sigma: SigmaMap = row.getAs(rIndex)
    val res = rules.evaluateRow(row, sigma)
    val objs: Array[Any] = row.toSeq.toArray
    objs(rIndex) = res
    new GenericRowWithSchema(objs, row.schema)
  }
}
