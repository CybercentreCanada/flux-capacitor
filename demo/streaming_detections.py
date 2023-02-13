from util import (
    tagged_telemetry_table,
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    process_telemetry_table,
    run,
    flux_capacitor,
    global_view,
    store_tagged_telemetry
)
import time

create_spark_session("streaming anomaly detections", 1)

global_view("sigma_rule_to_action")

# current time in milliseconds
ts = int(time.time() * 1000)

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

streaming_query = (
    final_df
    .writeStream
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", get_checkpoint_location(tagged_telemetry_table))
    .foreachBatch(foreach_batch_function)
    .start()
)

streaming_query.awaitTermination()
