import util
from util import (
    init_argparse,
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    run,
    flux_capacitor,
    global_view,
    store_tagged_telemetry
)
import time
import sys

def start_query(args):
    create_spark_session("streaming anomaly detections", 1)

    global_view("sigma_rule_to_action")

    # current time in milliseconds
    ts = int(time.time() * 1000)

    (
        get_spark()
        .readStream.format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-delete-snapshots", True)
        .load(util.process_telemetry_table)
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
        .queryName("detections")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(util.tagged_telemetry_table))
        .foreachBatch(foreach_batch_function)
        .start()
    )

    streaming_query.awaitTermination()




def main() -> int:
    args = init_argparse()
    start_query(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

