import time
from util import (
    tagged_telemetry_table,
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    process_telemetry_table,
    run,
    flux_capacitor,
    global_view,
    store_tagged_telemetry,
    find_temporal_proximity,
    suspected_anomalies_table,
    get_checkpoint_location,
    get_spark,
    find_ancestors,
    alerts_table,
    find_parents,
    print_anomalies,
    validate_events,
    store_alerts
)

create_spark_session("streaming anomaly detections", 1)

# current time in milliseconds
ts = int(time.time() * 1000)
global_view("sigma_rule_to_action")

def start_detections():
    (
        get_spark()
        .readStream.format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-delete-snapshots", True)
        .load(process_telemetry_table)
        .createOrReplaceTempView("process_telemetry_view")
    )

    # Step 1: evaluate discrete tags
    df = run("pre_flux_tagged_telemetry")
    # Step 2: time travel tags
    flux_capacitor(df)
    # Step 3: final evaluation of sigma rule
    # Now that we have the historical tags (for example parent tags)
    # we can evaluate rules which combine tags from the current row and its parent
    final_df = run("post_flux_eval_condition")

    def foreach_batch_function(batchdf, epoch_id):
        # Transform and write batchDF
        batchdf.persist()
        batchdf.createOrReplaceGlobalTempView("post_flux_eval_condition")
        run("publish_suspected_anomalies")
        store_tagged_telemetry(batchdf)
        batchdf.unpersist()

    checkpointLocation = get_checkpoint_location(tagged_telemetry_table) + "anomalies2"
    print(checkpointLocation)
    (
        final_df
        .writeStream
        .queryName("anomaly detections")
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", checkpointLocation)
        .foreachBatch(foreach_batch_function)
        .start()
    )

def query_suspected_anomalies(query_name, f):
    (
    get_spark()
    .readStream.format("iceberg")
    .option("stream-from-timestamp", ts)
    .option("streaming-skip-delete-snapshots", True)
    .load(suspected_anomalies_table)
    .writeStream
    .queryName(query_name)
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", get_checkpoint_location(alerts_table) + query_name)
    .foreachBatch(f)
    .start()
    )

def start_proximity_alert_builder():
    def foreach_batch_function(anomalies, epoch_id):
        # Transform and write batchDF
        anomalies.persist()
        prox = find_temporal_proximity(anomalies)
        prox.persist()
        print_anomalies("context for historical temporal proximity:", prox)
        validated_prox = validate_events(prox)
        print_anomalies("validated historical temporal proximity:", validated_prox)
        store_alerts(validated_prox)
        prox.unpersist(True)
        anomalies.unpersist(True)
        get_spark().catalog.clearCache()
        anomalies.sparkSession.catalog.clearCache()

    query_suspected_anomalies("temporal_proximity", foreach_batch_function)

def start_parent_alert_builder():
    def foreach_batch_function(anomalies, epoch_id):
        # Transform and write batchDF
        anomalies.persist()
        parents = find_parents(anomalies)
        parents.persist()
        print_anomalies("context for historical parents:", parents)
        validated_parents = validate_events(parents)
        print_anomalies("validated historical parents:", validated_parents)
        store_alerts(validated_parents)
        parents.unpersist(True)
        anomalies.unpersist(True)
        get_spark().catalog.clearCache()
        anomalies.sparkSession.catalog.clearCache()

    query_suspected_anomalies("parents", foreach_batch_function)

def start_ancestor_alert_builder():
    def foreach_batch_function(anomalies, epoch_id):
        # Transform and write batchDF
        anomalies.persist()
        ancestors = find_ancestors(anomalies)
        ancestors.persist()
        print_anomalies("context for historical ancestors:", ancestors)
        validated_ancestors = validate_events(ancestors)
        print_anomalies("validated historical ancestors:", validated_ancestors)
        store_alerts(validated_ancestors)
        ancestors.unpersist(True)
        anomalies.unpersist(True)
        get_spark().catalog.clearCache()
        anomalies.sparkSession.catalog.clearCache()

    query_suspected_anomalies("ancestors", foreach_batch_function)


start_detections()
start_ancestor_alert_builder()
# start_parent_alert_builder()
# start_proximity_alert_builder()

get_spark().streams.awaitAnyTermination()   

