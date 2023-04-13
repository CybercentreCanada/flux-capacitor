import sys
from demo.constants import init_globals, parse_args
import demo.constants as constants
from demo.util import (
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    make_name,
    monitor_query,
    run,
    flux_capacitor,
    create_view,
    create_dataframe
)
import time
import logging as logging
log = logging.getLogger(__name__)

def start_query(catalog, schema, trigger, verbose):
    init_globals(catalog, schema, verbose)
    name = make_name(schema, trigger, __file__)
    create_spark_session("streaming anomaly detections", 1)

    create_view("sigma_rule_to_action")

    # current time in milliseconds
    ts = int(time.time() * 1000)

    (
        get_spark()
        .readStream
        .format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-delete-snapshots", True)
        .option("streaming-skip-overwrite-snapshots", True)
        .load(constants.process_telemetry_table)
        .createOrReplaceTempView("process_telemetry_view")
    )

    # Step 1: Evaluate discrete tags (pattern match)
    df = create_dataframe("pre_flux_tagged_telemetry")
    # Step 2: Time travel tags
    flux_capacitor(df)
    # Step 3: Final evaluation of sigma rule
    # Now that we have the historical tags (for example parent tags)
    # we can evaluate rules which combine tags from the current row and its parent
    final_df = create_dataframe("post_flux_eval_condition")

    def foreach_batch_function(batchdf, epoch_id):
        # Transform and write batchDF
        batchdf.persist()
        batchdf.createOrReplaceGlobalTempView("post_flux_eval_condition")
        run("publish_suspected_anomalies")
        run("insert_into_tagged_telemetry")
        get_spark().catalog.clearCache()

    streaming_query = (
        final_df
        .writeStream
        .queryName("detections")
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(constants.tagged_telemetry_table) )
        .foreachBatch(foreach_batch_function)
        .start()
    )

    monitor_query(streaming_query, name)




def main() -> int:
    args = parse_args()
    start_query(args.catalog, args.schema, args.trigger, args.verbose)
    # we never expect to stop, if we do it indicates an error, return -1
    return -1
    
if __name__ == "__main__":
    sys.exit(main())

