package cccs.fluxcapacitor

import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.Logger

import org.apache.spark.sql.types.StructType
import com.google.common.hash.BloomFilter
import org.scalatest.BeforeAndAfter
import com.google.common.hash.Funnels
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.io.InputStreamReader
import com.google.common.io.CharStreams
import com.google.common.base.Charsets

import cccs.fluxcapacitor.RulesConf.SigmaMap

import java.lang
import org.apache.spark.sql.Row

class BasicTests extends AnyFunSuite with BeforeAndAfter {

  val log = Logger.getLogger(this.getClass)
  val rowSchema: StructType = StructType.fromDDL("""|id string, 
                                                    |parent_id string, 
                                                    |sigma map<string, map<string, boolean>>, 
                                                    |captured_folder_colname string
                                                    |""".stripMargin)
  val capacity = 10000
  var tagCache : MapTagCache = _

  //new MapTagCache
  var rules: Rules = _

  val rule1 = scala.collection.immutable.Map(
    "recon_cmd_a" -> false,
    "recon_cmd_b" -> false,
    "recon_cmd_c" -> false,
    "recon_cmd_d" -> false
  )

  val rule2 = scala.collection.immutable.Map(
    "recon_cmd_a" -> false,
    "recon_cmd_b" -> false,
    "recon_cmd_c" -> false,
    "recon_cmd_d" -> false
  )

  val rule3 = scala.collection.immutable.Map(
    "recon_cmd_a" -> false,
    "recon_cmd_b" -> false,
    "recon_cmd_c" -> false,
    "recon_cmd_d" -> false
  )

  val rule4 = scala.collection.immutable.Map(
    "recon_cmd_a" -> false,
    "recon_cmd_b" -> false,
    "recon_cmd_c" -> false,
    "recon_cmd_d" -> false
  )

  val rule5 = scala.collection.immutable.Map(
    "recon_cmd_a" -> false,
    "recon_cmd_b" -> false,
    "recon_cmd_c" -> false,
    "recon_cmd_d" -> false
  )

  val rule6 = scala.collection.immutable.Map(
    "recon_cmd_a" -> false,
    "recon_cmd_b" -> false,
    "recon_cmd_c" -> false,
    "recon_cmd_d" -> false
  )

  val allTagsFalse = scala.collection.immutable.Map(
    "rule1" -> rule1,
    "rule2" -> rule2,
    "rule3" -> rule3,
    "rule4" -> rule4,
    "rule5" -> rule5,
    "rule6" -> rule6,
  )

  def createRow(
      fqTags: Seq[String],
      id: String,
      parentId: String,
      capturedValue: String
  ) = {
    var sigmaResults = makeSigmaResults(fqTags)
    var values: Array[Any] = Array(id, parentId, sigmaResults, capturedValue)
    new GenericRowWithSchema(values, rowSchema)
  }

  def createOutputRow(input: Row, results: SigmaMap) = {
    var values: Array[Any] =
      Array(input.getString(0), input.getString(1), results, input.getString(3))
    new GenericRowWithSchema(values, rowSchema)
  }

  def totalNumTags(results: SigmaMap) = {
    results.values.map(tagMap => tagMap.size).sum
  }

  def getResults(row: Row): SigmaMap = {
    var index = row.fieldIndex("sigma")
    row.getAs(index)
  }

  def makeSigmaResults(fqTags: Seq[String]): SigmaMap = {
    val sigmaMap = makeSigmaFromFullyQualified(fqTags).map {
      case (ruleName, tagNames) =>
        (ruleName -> tagNames.map(tagName => (tagName, true)).toMap)
    }

    val ret = scala.collection.mutable
      .Map[String, scala.collection.immutable.Map[String, Boolean]]()

    allTagsFalse.keySet.foreach(ruleName => {
      val map1 = allTagsFalse.get(ruleName).get
      val map2 = sigmaMap.get(ruleName).getOrElse(Map())
      val tags = map1 ++ map2
      ret.put(ruleName, tags)
    })
    ret
  }

  def makeSigmaFromFullyQualified(fqTags: Seq[String]) = {
    // split strings in two
    fqTags
      .map(fqtag => fqtag.split("\\."))
      .map(parts => (parts(0), parts(1)))
      // group by first part, the rule name
      .groupBy(_._1)
      // the values in the map are a list of parts, change it to a list of tag names
      .mapValues(groupedItems => groupedItems.map(_._2))
  }

  def assertRow(row: Row, fqTags: Seq[String]) = {
    var expectedSigmaResults = makeSigmaFromFullyQualified(fqTags)
    var sigmaResults = getResults(row)

    for (ruleName <- expectedSigmaResults.keySet) {
      var expectedResult = expectedSigmaResults.get(ruleName).get
      var acutalResult = sigmaResults.get(ruleName).get
      expectedResult.foreach(tagName =>
        assert(acutalResult.get(tagName) == Some(true))
      )
      for (tagName <- acutalResult.keySet) {
        if (acutalResult.get(tagName) == Some(true)) {
          assert(expectedResult.contains(tagName))
        }
      }
    }
  }

  def loadYamlSpecification(fileName: String) = {
    var in = this.getClass().getClassLoader().getResourceAsStream(fileName)
    var spec = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8))
    RulesConf.load(spec)
  }

  def prettyPrintInput(row: Row) = {
    log.debug("input row:  " + row)
  }

  def prettyPrintOutput(row: Row) = {
    log.debug("output row: " + row)
  }

  def prettyPrintCache = {
    log.debug("tag cache:  " + tagCache)
  }

  def processRow(
      inputRowTags: Seq[String],
      expectedOutputTags: Seq[String],
      id: String = "0",
      parentId: String = "0",
      capturedValue: String = ""
  ) = {
    var inputRow = createRow(inputRowTags, id, parentId, capturedValue)
    prettyPrintInput(inputRow)
    val output = new RulesAdapter(rules).evaluateRow(inputRow)

    log.debug("==========================================")
    prettyPrintInput(inputRow)
    prettyPrintOutput(output)
    prettyPrintCache
    log.debug("==========================================")

    assertRow(output, expectedOutputTags)
  }

  def in(items: String*): Seq[String] = {
    items
  }

  def out(items: String*): Seq[String] = {
    items
  }

  def makeRules(fileName: String) = {
    tagCache = new MapTagCache
    val ruleConf = loadYamlSpecification(fileName)
    rules = new Rules(ruleConf, tagCache)
    rules
  }

  test("un-ordered set of events by host") {
    makeRules("rule5.yaml")
    processRow(in("rule5.recon_cmd_a"), out("rule5.recon_cmd_a"))
    processRow(
      in("rule5.recon_cmd_b"),
      out("rule5.recon_cmd_a", "rule5.recon_cmd_b")
    )
    processRow(
      in("rule5.recon_cmd_c"),
      out("rule5.recon_cmd_a", "rule5.recon_cmd_b", "rule5.recon_cmd_c")
    )
    processRow(
      in("rule5.recon_cmd_d"),
      out("rule5.recon_cmd_d")
    )
  
  }

  test("un-ordered set of events by host, with repeats") {
    makeRules("rule5.yaml")
    processRow(
      in("rule5.recon_cmd_a", "rule5.recon_cmd_c"),
      out("rule5.recon_cmd_a", "rule5.recon_cmd_c")
    )
    processRow(
      in("rule5.recon_cmd_b", "rule5.recon_cmd_d"),
      out(
        "rule5.recon_cmd_a",
        "rule5.recon_cmd_b",
        "rule5.recon_cmd_c",
        "rule5.recon_cmd_d"
      )
    )
  }

  test("ordered list of events by host") {
    /*
     * ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c, recon_cmd_d.
     * We don't need to remember recon_cmd_d so we omit it from the update
     * specification
     * see rule1.json
     * recon_cmd_b is only stored when a previous recon_cmd_a was true
     * recon_cmd_b has a put_condition on recon_cmd_a
     */

    makeRules("rule1.yaml")
    processRow(in("rule1.recon_cmd_b"), out("rule1.recon_cmd_b"))
    // b is not stored in bloom
    processRow(in("rule1.recon_cmd_c"), out("rule1.recon_cmd_c"))
    // c is not stored in bloom
    processRow(in("rule1.recon_cmd_a"), out("rule1.recon_cmd_a"))
    processRow(in("rule1.recon_cmd_a"), out("rule1.recon_cmd_a"))
    // a is stored in bloom
    processRow(
      in("rule1.recon_cmd_c"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_c")
    )
    // c is not stored in bloom
    processRow(
      in("rule1.recon_cmd_b"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_b")
    )
    // b is stored in bloom, since a is already stored
    processRow(
      in("rule1.recon_cmd_b"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_b")
    )
    // b again, no change
    processRow(
      in("rule1.recon_cmd_c"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_b", "rule1.recon_cmd_c")
    )
    // finally we saw c, it is stored and the output shows all tags were seen in
    // ordered.
    processRow(
      in("rule1.recon_cmd_c"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_b", "rule1.recon_cmd_c")
    )
    // none of the temporal tags (a,b,c) are on the input row, 
    // the flux capacitor skips evaluation of these rows
    processRow(
      in("rule1.recon_cmd_d"),
      out(
        "rule1.recon_cmd_d"      
      )
    )
  }

  test("ordered list of events by host, multiple repeats") {
    /*
     * ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c, recon_cmd_d.
     * We don't need to remember recon_cmd_d so we omit it from the update
     * specification
     * see rule1.json
     * recon_cmd_b is only stored when a previous recon_cmd_a was true
     * recon_cmd_b has a put_condition on recon_cmd_a
     */
    makeRules("rule1.yaml")
    processRow(
      in("rule1.recon_cmd_b", "rule1.recon_cmd_c"),
      out("rule1.recon_cmd_b", "rule1.recon_cmd_c")
    )
    // b, c is not stored in bloom
    processRow(
      in("rule1.recon_cmd_c", "rule1.recon_cmd_a"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_c")
    )
    // a stored in bloom
    processRow(
      in("rule1.recon_cmd_b", "rule1.recon_cmd_c"),
      out("rule1.recon_cmd_a", "rule1.recon_cmd_b", "rule1.recon_cmd_c")
    )
    // none of the temporal tags (a,b,c) are on the input row, 
    // the flux capacitor skips evaluation of these rows
    processRow(
      in("rule1.recon_cmd_d"),
      out(
        "rule1.recon_cmd_d"
      )
    )
  }

  test("propagate to child only") {
    makeRules("rule4.yaml")
    processRow(
      in("rule4.process_feature1"),
      out("rule4.process_feature1"),
      id = "1",
      parentId = "nothing"
    )
    // nothing stored, not a feature we want to remember
    System.out.println("HERE " + this.rules)
    processRow(in(), out(), id = "11", parentId = "1")

    processRow(
      in("rule4.parent_process_feature1"),
      out("rule4.parent_process_feature1"),
      id = "2",
      parentId = "nothing"
    )
    // parent_process_feature1 is a feature we want to remember
    processRow(
      in("rule4.parent_process_feature1"),
      out("rule4.parent_process_feature1"),
      id = "3",
      parentId = "nothing"
    )
    // parent_process_feature1 is a feature we want to remember
    processRow(
      in("rule4.process_feature1"),
      out("rule4.process_feature1", "rule4.parent_process_feature1"),
      id = "13",
      parentId = "3"
    )
    // we should find our parent's feature, even if we don't have any
    processRow(
      in(),
      out("rule4.parent_process_feature1"),
      id = "13",
      parentId = "3"
    )
    // parent does not exists
    processRow(in(), out(), id = "14", parentId = "4")
    // we should find our parent's feature
    processRow(
      in(),
      out("rule4.parent_process_feature1"),
      id = "12",
      parentId = "2"
    )
    // we should not find our ancestor's feature
    processRow(in(), out(), id = "133", parentId = "13")

  }

  test("propagate to all decendents") {
    makeRules("rule3.yaml")
    processRow(
      in("rule3.process_feature1"),
      out("rule3.process_feature1"),
      id = "1",
      parentId = "null"
    )
    // nothing stored, not a feature we want to remember
    System.out.println("HERE " + this.rules)
    processRow(in(), out(), "11", "1")

    processRow(
      in("rule3.ancestor_process_feature1"),
      out("rule3.ancestor_process_feature1"),
      id = "2",
      parentId = "null"
    )
    // ancestor_process_feature1 is a feature we want to remember
    processRow(
      in("rule3.ancestor_process_feature1"),
      out("rule3.ancestor_process_feature1"),
      id = "3",
      parentId = "null"
    )
    // ancestor_process_feature1 is a feature we want to remember
    processRow(
      in("rule3.process_feature1"),
      out("rule3.process_feature1", "rule3.ancestor_process_feature1"),
      id = "13",
      parentId = "3"
    )
    // parent does not exists
    processRow(in(), out(), id = "14", parentId = "4")
    // we should find our parent's feature
    processRow(
      in(),
      out("rule3.ancestor_process_feature1"),
      id = "12",
      parentId = "2"
    )
    // we should also find our ancestor's feature
    processRow(
      in(),
      out("rule3.ancestor_process_feature1"),
      id = "133",
      parentId = "13"
    )

  }

  test("ordered list of events by host and for particular context key") {

    makeRules("rule6.yaml")
    processRow(
      in("rule6.recon_cmd_b"),
      out("rule6.recon_cmd_b"),
      capturedValue = "folderA"
    )
    // b is not stored in bloom
    processRow(
      in("rule6.recon_cmd_c"),
      out("rule6.recon_cmd_c"),
      capturedValue = "folderA"
    )
    // c is not stored in bloom
    processRow(
      in("rule6.recon_cmd_a"),
      out("rule6.recon_cmd_a"),
      capturedValue = "folderA"
    )
    processRow(
      in("rule6.recon_cmd_a"),
      out("rule6.recon_cmd_a"),
      capturedValue = "folderA"
    )
    // a is stored in bloom
    processRow(
      in("rule6.recon_cmd_c"),
      out("rule6.recon_cmd_a", "rule6.recon_cmd_c"),
      capturedValue = "folderA"
    )
    // c is not stored in bloom
    processRow(
      in("rule6.recon_cmd_b"),
      out("rule6.recon_cmd_a", "rule6.recon_cmd_b"),
      capturedValue = "folderA"
    )
    // b is stored in bloom, since a is already stored
    processRow(
      in("rule6.recon_cmd_b"),
      out("rule6.recon_cmd_a", "rule6.recon_cmd_b"),
      capturedValue = "folderA"
    )
    // b again, no change
    processRow(
      in("rule6.recon_cmd_c"),
      out("rule6.recon_cmd_a", "rule6.recon_cmd_b", "rule6.recon_cmd_c"),
      capturedValue = "folderA"
    )
    // finally we saw c, it is stored and the output shows all tags were seen in
    // ordered.
    // none of the temporal tags (a,b,c) are on the input row, 
    // the flux capacitor skips evaluation of these rows
    processRow(
      in("rule6.recon_cmd_d"),
      out(
        "rule6.recon_cmd_d"
      ),
      capturedValue = "folderA"
    )

    // tags are stored according to context (captured_folder_colname)
    // a and b are stored for context folderA, but not for folder B
    processRow(
      in("rule6.recon_cmd_c"),
      out("rule6.recon_cmd_c"),
      capturedValue = "folderB"
    )

    processRow(
      in("rule6.recon_cmd_a"),
      out("rule6.recon_cmd_a"),
      capturedValue = "folderB"
    )
    // a is stored in bloom
    processRow(
      in("rule6.recon_cmd_b"),
      out("rule6.recon_cmd_a", "rule6.recon_cmd_b"),
      capturedValue = "folderB"
    )
    processRow(
      in("rule6.recon_cmd_c"),
      out("rule6.recon_cmd_a", "rule6.recon_cmd_b", "rule6.recon_cmd_c"),
      capturedValue = "folderB"
    )
    processRow(
      in("rule6.recon_cmd_d"),
      out(
        "rule6.recon_cmd_d"
      ),
      capturedValue = "folderB"
    )

  }

}
