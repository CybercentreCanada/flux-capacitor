
import sys
from demo.streaming_alert_builder import find_temporal_proximity, find_parents, find_ancestors
import demo.constants as constants
from demo.util import (
    create_view,
    get_spark,
    create_spark_session,
    render_file,
    validate_events,
    flux_capacitor,
    drop,
    run,
    print_anomalies,
    print_final_results,
    print_telemetry,
)


def create_or_replace_tables(args):
    drop(constants.tagged_telemetry_table)
    drop(constants.process_telemetry_table)
    drop(constants.suspected_anomalies_table)
    drop(constants.alerts_table)
    run("create_tagged_telemetry_table")
    run("create_process_telemetry_table")
    run("create_suspected_anomalies_table")
    run("create_alert_table")
    run("populate_process_telemetry_table")


def run_detections(args):
    print("The telemetry table consists of Windows start-process events:")
    df = get_spark().table(constants.process_telemetry_table)
    df.select("timestamp", "id", "parent_id", "Commandline").orderBy("timestamp").show(truncate=False)
    df.createOrReplaceTempView("process_telemetry_view")

    # Step 1: evaluate discrete tags
    df = run("pre_flux_tagged_telemetry")
    print("Step 1: pre-flux")
    print(f"Statement applied: {render_file('pre_flux_tagged_telemetry')}")
    print_telemetry("Result: map of tags for each rule:", df)

    # Step 2: time travel tags
    df = flux_capacitor(df)
    print_telemetry("Step2: flux-capacitor, previously cached tags are retrieved and copied into the current row/event:", df)

    # Step 3: final evaluation of sigma rule
    # Now that we have the historical tags (for example parent tags)
    # we can evaluate rules which combine tags from the current row and its parent
    post_flux_eval_condition = create_view("post_flux_eval_condition")
    post_flux_eval_condition.persist()
    print("Step3: post-flux, evaluate the final sigma condition")
    print(f"Statement applied: {render_file('post_flux_eval_condition')}")
    print_final_results("Result: sigma_final is a list of firing sigma rule names:", post_flux_eval_condition)

    create_view("sigma_rule_to_action")
    print("Events that tigger a sigma rule are published to the suspected_anomalies table")
    print(f"Statement to publish anomalies: {render_file('publish_suspected_anomalies')}")
    run("publish_suspected_anomalies")
    run("insert_into_tagged_telemetry")


def run_parents_alert_builder(args):
    anomalies = get_spark().table(constants.suspected_anomalies_table)
    anomalies.persist()
    print_anomalies("suspected anomalies queue:", anomalies)
    create_view("sigma_rule_to_action")
    parents = find_parents(anomalies)
    parents.persist()
    print_anomalies("events to validate for historical parents:", parents)
    validated_parents = validate_events(parents)
    print_anomalies("validated historical parents:", validated_parents)
    run("insert_into_alerts")
    parents.unpersist()
    anomalies.unpersist()


def run_ancestors_alert_builder(args):
    anomalies = get_spark().table(constants.suspected_anomalies_table)
    anomalies.persist()
    print_anomalies("suspected anomalies queue:", anomalies)
    create_view("sigma_rule_to_action")
    ancestors = find_ancestors(anomalies)
    ancestors.persist()
    print_anomalies("events to validate for historical ancestors:", ancestors)
    validated_ancestors = validate_events(ancestors)
    print_anomalies("validated historical ancestors:", validated_ancestors)
    run("insert_into_alerts")
    ancestors.unpersist()
    anomalies.unpersist()

def run_temporal_proximity_alert_builder(args):
    anomalies = get_spark().table(constants.suspected_anomalies_table)
    anomalies.persist()
    print_anomalies("suspected anomalies queue:", anomalies)
    create_view("sigma_rule_to_action")
    prox = find_temporal_proximity(anomalies)
    prox.persist()
    print_anomalies("events to validate for historical temporal proximity:", prox)
    validated_prox = validate_events(prox)
    print_anomalies("validated historical temporal proximity:", validated_prox)
    run("insert_into_alerts")
    prox.unpersist()
    anomalies.unpersist()





def main() -> int:
    args = constants.init_argparse()
    create_spark_session("integration test", 1)
    print("\n\n================ create_or_replace_tables =======================")
    create_or_replace_tables(args)
    print("\n\n================ run_detections =======================")
    run_detections(args)
    print("\n\n================ run_temporal_proximity_alert_builder =======================")
    run_temporal_proximity_alert_builder(args)
    print("\n\n================ run_parents_alert_builder =======================")
    run_parents_alert_builder(args)
    print("\n\n================ run_ancestors_alert_builder =======================")
    run_ancestors_alert_builder(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

