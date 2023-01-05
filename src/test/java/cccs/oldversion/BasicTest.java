package cccs.oldversion;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.shaded.org.jline.utils.InputStreamReader;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.io.CharStreams;

import cccs.oldversion.Rules;
import cccs.oldversion.RulesConf;




/*
parent_key,
current_key,
capturedxyz,

sigma =
(
rule1 -> (
        recon_cmd_a -> false,
        recon_cmd_b -> false,
        recon_cmd_c -> false
    )
rule2 -> (
        recon_cmd_a -> false,
        recon_cmd_b -> false,
        recon_cmd_c -> false
    )
rule3 -> (
        selection_parent -> false,
        selection_img -> false,
    )
rule4 -> (
        selection_parent -> false,
        selection_img -> false,
    )
)
*/

public class BasicTest {
    static Logger log = Logger.getLogger(BasicTest.class.getName());
    StructType rowSchema;
    int capacity = 10_000;
    BloomFilter<CharSequence> bloom;
    private Rules rules;

    @Before
    public void setUp() throws Exception {
        rowSchema = StructType.fromDDL(
                "id string, parent_id string, sigma map<string, map<string, boolean>>, captured_folder_colname string");
        // System.out.println(rowSchema);
        bloom = BloomFilter.create(Funnels.stringFunnel(), capacity, 0.01);
    }

    public Map<String, Boolean> getRule(String ruleName, Map<String, Map<String, Boolean>> sigmaResults) {
        Map<String, Boolean> rule = sigmaResults.get(ruleName);
        if (rule == null) {
            rule = new HashMap<>();
            sigmaResults.put(ruleName, rule);
        }
        return rule;
    }

    public Row createRow(String[] fqTags, String id, String parentId, String capturedValue) {
        java.util.Map<String, Map<String, Boolean>> sigmaResults = new HashMap<>();
        for (String fqTag : fqTags) {
            String[] parts = fqTag.split("\\.");
            String ruleName = parts[0];
            String tagName = parts[1];
            Map<String, Boolean> rule = getRule(ruleName, sigmaResults);
            rule.put(tagName, true);
        }
        Object[] values = { id, parentId, sigmaResults, capturedValue };
        Row row = new GenericRowWithSchema(values, rowSchema);
        return row;
    }

    public Row createOutputRow(Row input, Map<String, Map<String, Boolean>> results) {
        Object[] values = { input.getString(0),
                input.getString(1),
                results,
                input.getString(3) };
        Row row = new GenericRowWithSchema(values, rowSchema);
        return row;
    }

    public int totalNumTags(Map<String, Map<String, Boolean>> results) {
        int c = 0;
        for (Map<String, Boolean> ruleTags : results.values()) {
            c += ruleTags.size();
        }
        return c;
    }

    public void assertRow(Row row, String[] fqTags) {
        java.util.Map<String, Map<String, Boolean>> expectedSigmaResults = new HashMap<>();
        for (String fqTag : fqTags) {
            String[] parts = fqTag.split("\\.");
            String ruleName = parts[0];
            String tagName = parts[1];
            Map<String, Boolean> rule = getRule(ruleName, expectedSigmaResults);
            rule.put(tagName, true);
        }
        Map<String, Map<String, Boolean>> sigmaResults = getResults(row);

        for (String ruleName : expectedSigmaResults.keySet()) {
            Map<String, Boolean> expectedResult = expectedSigmaResults.get(ruleName);
            Map<String, Boolean> acutalResult = sigmaResults.get(ruleName);
            for (String tag : expectedResult.keySet()) {
                if (acutalResult.get(tag) == null) {
                    assertTrue("Tag " + tag + " missing from results " + acutalResult, false);
                }
                if (acutalResult.get(tag) == false) {
                    assertTrue("Tag " + tag + " is false in results " + acutalResult, false);
                }
            }
            // assertThat(acutalResult.entrySet(), equalTo(expectedResult.entrySet()));
            for (String tag : acutalResult.keySet()) {
                System.out.println("................................................");
                if (acutalResult.get(tag) != null) {
                    if (acutalResult.get(tag) == true && expectedResult.get(tag) == null) {
                        assertTrue("Tag " + tag + " is true in results " + acutalResult, false);
                    }
                }
            }
        }
    }

    public JSONObject loadRuleSpecification(String ruleName) throws Exception {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(ruleName);
        String jsonRule = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));
        // System.out.println(jsonRule);
        JSONObject spec = new JSONObject(jsonRule);
        return spec;
    }
    public RulesConf loadYamlSpecification(String fileName) throws Exception {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(fileName);
        String spec = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));
        return RulesConf.load(spec);
    }

    public Map<String, Map<String, Boolean>> getResults(Row row) {
        int s = row.fieldIndex("sigma");
        Map<String, Map<String, Boolean>> sigmaResults = (Map<String, Map<String, Boolean>>) row.get(s);
        return sigmaResults;
    }

    public void prettyPrintInput(Row row) {
        log.debug(">> " + row);
    }

    public void prettyPrintOutput(Row row) {
        log.debug("<< " + row);
    }

    public void processRow(String[] inputRowTags, String[] expectedOutputTags) {
        processRow(inputRowTags, expectedOutputTags, "0", "0");
    }

    public void processRow(String[] inputRowTags, String[] expectedOutputTags, String capturedValue) {
        processRow(inputRowTags, expectedOutputTags, "0", "0", capturedValue);
    }

    public void processRow(String[] inputRowTags, String[] expectedOutputTags, String id, String parentId) {
        processRow(inputRowTags, expectedOutputTags, id, parentId, "");
    }

    public void processRow(String[] inputRowTags, String[] expectedOutputTags, String id, String parentId,
            String capturedValue) {
        Row inputRow = createRow(inputRowTags, id, parentId, capturedValue);
        prettyPrintInput(inputRow);
        int s = inputRow.fieldIndex("sigma");
        Map<String, Map<String, Boolean>> sigma = (Map<String, Map<String, Boolean>>) inputRow.get(s);
        Map<String, Map<String, Boolean>> results = rules.evaluateRow(inputRow, sigma);
        Row output = createOutputRow(inputRow, results);
        prettyPrintOutput(output);
        assertRow(output, expectedOutputTags);
        log.debug("==========================================");
    }

    public String[] in(String... items) {
        return items;
    }

    public String[] out(String... items) {
        return items;
    }


    @Test
    public void testRule5UnorderedEvents() throws Exception {
        try{
        this.rules = new Rules(loadYamlSpecification("rule5.yaml"), bloom);
        processRow(in("rule5.recon_cmd_a"), out("rule5.recon_cmd_a"));
        processRow(in("rule5.recon_cmd_b"),
                out("rule5.recon_cmd_a", "rule5.recon_cmd_b"));
        processRow(in("rule5.recon_cmd_c"),
                out("rule5.recon_cmd_a", "rule5.recon_cmd_b", "rule5.recon_cmd_c"));
        }catch(Exception e){
            e.printStackTrace();
            fail(e.getMessage());

        }
    }

    @Test
    public void testRule5UnorderedEventsMultipleTagsOnSameRow() throws Exception {
        this.rules = new Rules(loadYamlSpecification("rule5.yaml"), bloom);
        processRow(in("rule5.recon_cmd_a", "rule5.recon_cmd_c"),
                out("rule5.recon_cmd_a", "rule5.recon_cmd_c"));
        processRow(in("rule5.recon_cmd_b", "rule5.recon_cmd_d"),
                out("rule5.recon_cmd_a", "rule5.recon_cmd_b", "rule5.recon_cmd_c", "rule5.recon_cmd_d"));
    }

    @Test
    public void testRule1OrderedList() throws Exception {
        /*
         * ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c, recon_cmd_d.
         * We don't need to remember recon_cmd_d so we omit it from the update
         * specification
         * see rule1.json
         * recon_cmd_b is only stored when a previous recon_cmd_a was true
         * recon_cmd_b has a put_condition on recon_cmd_a
         */
        try{
        this.rules = new Rules(loadYamlSpecification("rule1.yaml"), bloom);
        processRow(in("rule1.recon_cmd_b"), out("rule1.recon_cmd_b"));
        // b is not stored in bloom
        processRow(in("rule1.recon_cmd_c"), out("rule1.recon_cmd_c"));
        // c is not stored in bloom
        processRow(in("rule1.recon_cmd_a"), out("rule1.recon_cmd_a"));
        processRow(in("rule1.recon_cmd_a"), out("rule1.recon_cmd_a"));
        // a is stored in bloom
        processRow(in("rule1.recon_cmd_c"), out("rule1.recon_cmd_a",
                "rule1.recon_cmd_c"));
        // c is not stored in bloom
        processRow(in("rule1.recon_cmd_b"), out("rule1.recon_cmd_a",
                "rule1.recon_cmd_b"));
        // b is stored in bloom, since a is already stored
        processRow(in("rule1.recon_cmd_b"), out("rule1.recon_cmd_a",
                "rule1.recon_cmd_b"));
        // b again, no change
        processRow(in("rule1.recon_cmd_c"), out("rule1.recon_cmd_a",
                "rule1.recon_cmd_b", "rule1.recon_cmd_c"));
        // finally we saw c, it is stored and the output shows all tags were seen in
        // ordered.
        processRow(in("rule1.recon_cmd_d"),
                out("rule1.recon_cmd_a", "rule1.recon_cmd_b", "rule1.recon_cmd_c",
                        "rule1.recon_cmd_d"));
        }
        catch(Exception e){
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRule1OrderedMultipleOnSameRow() throws Exception {
        /*
         * ordered temporal rules recon_cmd_a, recon_cmd_b, recon_cmd_c, recon_cmd_d.
         * We don't need to remember recon_cmd_d so we omit it from the update
         * specification
         * see rule1.json
         * recon_cmd_b is only stored when a previous recon_cmd_a was true
         * recon_cmd_b has a put_condition on recon_cmd_a
         */
        try {
            this.rules = new Rules(loadYamlSpecification("rule1.yaml"), bloom);
            processRow(in("rule1.recon_cmd_b", "rule1.recon_cmd_c"),
                    out("rule1.recon_cmd_b", "rule1.recon_cmd_c"));
            // b, c is not stored in bloom
            processRow(in("rule1.recon_cmd_c", "rule1.recon_cmd_a"),
                    out("rule1.recon_cmd_a", "rule1.recon_cmd_c"));
            // a stored in bloom
            processRow(in("rule1.recon_cmd_b", "rule1.recon_cmd_c"),
                    out("rule1.recon_cmd_a", "rule1.recon_cmd_b", "rule1.recon_cmd_c"));
            processRow(in("rule1.recon_cmd_d"),
                    out("rule1.recon_cmd_a", "rule1.recon_cmd_b", "rule1.recon_cmd_c",
                            "rule1.recon_cmd_d"));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());

        }
    }

    @Test
    public void testRule5UnorderedEventsWithRepeats() throws Exception {
        this.rules = new Rules(loadYamlSpecification("rule5.yaml"), bloom);
        processRow(in("rule5.recon_cmd_b"), out("rule5.recon_cmd_b"));
        processRow(in("rule5.recon_cmd_c"), out("rule5.recon_cmd_b",
                "rule5.recon_cmd_c"));
        processRow(in("rule5.recon_cmd_c"), out("rule5.recon_cmd_b",
                "rule5.recon_cmd_c"));
        processRow(in("rule5.recon_cmd_a"), out("rule5.recon_cmd_a",
                "rule5.recon_cmd_b", "rule5.recon_cmd_c"));
    }

    @Test
    public void testRule4() throws Exception {
        this.rules = new Rules(loadYamlSpecification("rule4.yaml"), bloom);
        processRow(
                in("rule4.process_feature1"),
                out("rule4.process_feature1"), "1", "nothing");
        // nothing stored, not a feature we want to remember
        System.out.println("HERE " + this.rules);
        processRow(in(), out(), "11", "1");

        processRow(in("rule4.parent_process_feature1"),
                out("rule4.parent_process_feature1"), "2", "nothing");
        // parent_process_feature1 is a feature we want to remember
        processRow(in("rule4.parent_process_feature1"),
                out("rule4.parent_process_feature1"), "3", "nothing");
        // parent_process_feature1 is a feature we want to remember
        processRow(in("rule4.process_feature1"),
                out("rule4.process_feature1", "rule4.parent_process_feature1"),
                "13", "3");
        // we should find our parent's feature, even if we don't have any
        processRow(in(),
                out("rule4.parent_process_feature1"),
                "13", "3");
        // parent does not exists
        processRow(in(), out(), "14", "4");
        // we should find our parent's feature
        processRow(in(), out("rule4.parent_process_feature1"), "12", "2");
        // we should not find our ancestor's feature
        processRow(in(), out(), "133", "13");

    }

    @Test
    public void testRule3() throws Exception {
        this.rules = new Rules(loadYamlSpecification("rule3.yaml"), bloom);
        processRow(in("rule3.process_feature1"), out("rule3.process_feature1"), "1",
                "null");
        // nothing stored, not a feature we want to remember
        System.out.println("HERE " + this.rules);
        processRow(in(), out(), "11", "1");

        processRow(in("rule3.ancestor_process_feature1"),
                out("rule3.ancestor_process_feature1"), "2", "null");
        // ancestor_process_feature1 is a feature we want to remember
        processRow(in("rule3.ancestor_process_feature1"),
                out("rule3.ancestor_process_feature1"), "3", "null");
        // ancestor_process_feature1 is a feature we want to remember
        processRow(in("rule3.process_feature1"), out("rule3.process_feature1",
                "rule3.ancestor_process_feature1"), "13",
                "3");
        // parent does not exists
        processRow(in(), out(), "14", "4");
        // we should find our parent's feature
        processRow(in(), out("rule3.ancestor_process_feature1"), "12", "2");
        // we should also find our ancestor's feature
        processRow(in(), out("rule3.ancestor_process_feature1"), "133", "13");

    }

    @Test
    public void testRule6OrderedListOfEventsKeyedToCapturedValue() throws Exception {

        this.rules = new Rules(loadYamlSpecification("rule6.yaml"), bloom);
        processRow(in("rule6.recon_cmd_b"), out("rule6.recon_cmd_b"), "folderA");
        // b is not stored in bloom
        processRow(in("rule6.recon_cmd_c"), out("rule6.recon_cmd_c"), "folderA");
        // c is not stored in bloom
        processRow(in("rule6.recon_cmd_a"), out("rule6.recon_cmd_a"), "folderA");
        processRow(in("rule6.recon_cmd_a"), out("rule6.recon_cmd_a"), "folderA");
        // a is stored in bloom
        processRow(in("rule6.recon_cmd_c"), out("rule6.recon_cmd_a",
                "rule6.recon_cmd_c"), "folderA");
        // c is not stored in bloom
        processRow(in("rule6.recon_cmd_b"), out("rule6.recon_cmd_a",
                "rule6.recon_cmd_b"), "folderA");
        // b is stored in bloom, since a is already stored
        processRow(in("rule6.recon_cmd_b"), out("rule6.recon_cmd_a",
                "rule6.recon_cmd_b"), "folderA");
        // b again, no change
        processRow(in("rule6.recon_cmd_c"), out("rule6.recon_cmd_a",
                "rule6.recon_cmd_b", "rule6.recon_cmd_c"),
                "folderA");
        // finally we saw c, it is stored and the output shows all tags were seen in
        // ordered.
        processRow(in("rule6.recon_cmd_d"),
                out("rule6.recon_cmd_a", "rule6.recon_cmd_b", "rule6.recon_cmd_c",
                        "rule6.recon_cmd_d"),
                "folderA");

        // tags are stored according to context (captured_folder_colname)
        // a and b are stored for context folderA, but not for folder B
        processRow(in("rule6.recon_cmd_c"), out("rule6.recon_cmd_c"), "folderB");

        processRow(in("rule6.recon_cmd_a"), out("rule6.recon_cmd_a"), "folderB");
        // a is stored in bloom
        processRow(in("rule6.recon_cmd_b"), out("rule6.recon_cmd_a",
                "rule6.recon_cmd_b"), "folderB");
        processRow(in("rule6.recon_cmd_c"), out("rule6.recon_cmd_a",
                "rule6.recon_cmd_b", "rule6.recon_cmd_c"),
                "folderB");
        processRow(in("rule6.recon_cmd_d"),
                out("rule6.recon_cmd_a", "rule6.recon_cmd_b", "rule6.recon_cmd_c",
                        "rule6.recon_cmd_d"),
                "folderB");

    }

}