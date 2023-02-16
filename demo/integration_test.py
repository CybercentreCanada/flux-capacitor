
import sys
from streaming_proximity_alert_builder import find_temporal_proximity
from streaming_parent_alert_builder import find_parents
from streaming_ancestor_alert_builder import find_ancestors
import util
from util import (
    get_spark,
    create_spark_session,
    init_argparse,
    print_anomalies,
    validate_events,
    store_tagged_telemetry,
    flux_capacitor,
    print_telemetry,
    print_final_results,
    drop,
    run,
    store_alerts
)

def run_integration_test(args):
    create_spark_session("integration test", 1)

    drop(util.tagged_telemetry_table)
    drop(util.process_telemetry_table)
    drop(util.suspected_anomalies_table)
    drop(util.alerts_table)
    run("create_tagged_telemetry_table")
    run("alter_tagged_telemetry_table")
    run("create_process_telemetry_table")
    run("create_suspected_anomalies_table")
    run("create_alert_table")

    run("populate_process_telemetry_table")
    # get_spark().sql("(select id as value, id / 1000 as ts from range(1,200000))").createOrReplaceTempView("rate_view")
    # df = run("generate_synthetic_telemetry")
    # df.writeTo(process_telemetry_table).append() 


    print("process table:")
    df = get_spark().table(util.process_telemetry_table)
    df.select("timestamp", "id", "parent_id", "Commandline").show(truncate=False)
    df.createOrReplaceTempView("process_telemetry_view")

    # Step 1: evaluate discrete tags
    df = run("pre_flux_tagged_telemetry")
    print_telemetry("with discrete tags:", df)

    # Step 2: time travel tags
    df = flux_capacitor(df)
    print_telemetry("with temporal tags:", df)

    # Step 3: final evaluation of sigma rule
    # Now that we have the historical tags (for example parent tags)
    # we can evaluate rules which combine tags from the current row and its parent
    post_flux_eval_condition = util.global_view("post_flux_eval_condition")
    post_flux_eval_condition.persist()
    post_flux_eval_condition.printSchema()

    print_final_results("with final eval tag:", post_flux_eval_condition)

    util.global_view("sigma_rule_to_action")

    run("publish_suspected_anomalies")
    store_tagged_telemetry(post_flux_eval_condition)

    anomalies = get_spark().table(util.suspected_anomalies_table)
    anomalies.persist()
    print_anomalies("anomalies found:", anomalies)
    util.global_view("sigma_rule_to_action")
    parents = find_parents(anomalies)
    parents.persist()
    print_anomalies("context for historical parents:", parents)
    validated_parents = validate_events(parents)
    print_anomalies("validated historical parents:", validated_parents)
    store_alerts(validated_parents)
    parents.unpersist()


    ancestors = find_ancestors(anomalies)
    ancestors.persist()
    util.global_view("sigma_rule_to_action")
    print_anomalies("context for historical ancestors:", ancestors)
    validated_ancestors = validate_events(ancestors)
    print_anomalies("validated historical ancestors:", validated_ancestors)
    store_alerts(validated_ancestors)
    ancestors.unpersist()

    prox = find_temporal_proximity(anomalies)
    prox.persist()
    util.global_view("sigma_rule_to_action")
    print_anomalies("context for historical temporal proximity:", prox)
    validated_prox = validate_events(prox)
    print_anomalies("validated historical temporal proximity:", validated_prox)
    store_alerts(validated_prox)
    prox.unpersist()

    anomalies.unpersist()





def main() -> int:
    args = init_argparse()
    run_integration_test(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

