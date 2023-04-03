from constants import init_argparse
import constants
from util import (
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    make_name,
    monitor_query,
    run,
    flux_capacitor,
    create_view,
    store_tagged_telemetry,
    create_dataframe
)
import time
import sys

def start_query(args):
    create_spark_session("streaming anomaly detections",  1)

    create_view("sigma_rule_to_action")

    # current time in milliseconds
    ts = int(time.time() * 1000)

    (
        get_spark()
        .readStream
        .format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-delete-snapshots", True)
        .load(constants.process_telemetry_table)
        .repartition(15)
        .createOrReplaceTempView("process_telemetry_view")
    )

    # Step 1: Evaluate discrete tags (pattern match)
    df = create_dataframe("pre_flux_tagged_telemetry")
    # Step 2: Time travel tags
    #flux_capacitor(df)
    df = df.withColumn("sigma", df.sigma_pre_flux)
    df.createOrReplaceGlobalTempView("flux_capacitor_output")
    # Step 3: Final evaluation of sigma rule
    # Now that we have the historical tags (for example parent tags)
    # we can evaluate rules which combine tags from the current row and its parent
    final_df = create_dataframe("post_flux_eval_condition")

    def foreach_batch_function(batchdf, epoch_id):
        # Transform and write batchDF
        batchdf.persist()
        batchdf.createOrReplaceGlobalTempView("post_flux_eval_condition")
        run("publish_suspected_anomalies")
        store_tagged_telemetry(batchdf)
        get_spark().catalog.clearCache()

    streaming_query = (
        final_df
        .writeStream
        .queryName("detections")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(constants.tagged_telemetry_table) + "noflux3")
        .foreachBatch(foreach_batch_function)
        .start()
    )

    monitor_query(streaming_query, args.name)




def main() -> int:
    args = init_argparse()
    args.name = make_name(args, __file__)
    start_query(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

