package cccs.oldversion;

import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Map;

public class RulesAdapter {

  private Rules rules;

  public RulesAdapter(Rules rules) {
    this.rules = rules;
  }

  public static java.util.Map<String, Boolean> scalaTagsToJava(Map<String, Boolean> scalaTags) {
    return JavaConversions.mapAsJavaMap(scalaTags);
  }

  public static java.util.Map<String, java.util.Map<String, Boolean>> scalaResultsToJava(
      Map<String, Map<String, Boolean>> scalaResults) {
    return JavaConversions.mapAsJavaMap(scalaResults)
        .entrySet().stream()
        .map(e -> ImmutablePair.of(e.getKey(), scalaTagsToJava(e.getValue())))
        .collect(Collectors.toMap(p -> p.left, p -> p.right));
  }

  public static Map<String, Map<String, Boolean>> javaResultsToScala(
      java.util.Map<String, java.util.Map<String, Boolean>> javaResults) {
    java.util.Map<String, Map<String, Boolean>> tmp = javaResults.entrySet().stream()
        .map(e -> ImmutablePair.of(e.getKey(), javaTagsToScala(e.getValue())))
        .collect(Collectors.toMap(p -> p.left, p -> p.right));
    return JavaConversions.mapAsScalaMap(tmp);
  }

  public static Map<String, Boolean> javaTagsToScala(java.util.Map<String, Boolean> javaTags) {
    return JavaConversions.mapAsScalaMap(javaTags);
  }

  public Row evaluateRow(Row row) {
    int rIndex = row.fieldIndex("sigma");
    Map<String, Map<String, Boolean>> scalaResults = (Map<String, Map<String, Boolean>>) row.get(rIndex);
    Map<String, Map<String, Boolean>> scalaEvalResults = evaluateRowAndResults(row, scalaResults);
    Object[] objs = rowObjects(row);
    objs[rIndex] = scalaEvalResults;
    return new GenericRowWithSchema(objs, row.schema());
  }

  public Map<String, Map<String, Boolean>> evaluateRowAndResults(Row row, Map<String, Map<String, Boolean>> scalaResults) {
    java.util.Map<String, java.util.Map<String, Boolean>> javaEvalResults = this.rules.evaluateRow(row,
        scalaResultsToJava(scalaResults));
        Map<String, Map<String, Boolean>> scalaEvalResults = javaResultsToScala(javaEvalResults);
        return scalaEvalResults;
  }

  public static Object[] rowObjects(Row row) {
    return JavaConverters.seqAsJavaList(row.toSeq()).toArray();
  }

}
