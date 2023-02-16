import sys
import time
import util
from util import (
    init_argparse,
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    global_view,
    print_anomalies,
    validate_events,
    store_alerts
)


def find_temporal_proximity(anomalies):
    anomalies.createOrReplaceGlobalTempView("anomalies")
    temporal_anomalies = anomalies.where("detection_action = 'temporal'")
    if temporal_anomalies.count() == 0:
        return temporal_anomalies
    return get_spark().sql(f"""
        select
            e.*,
            a.detection_id,
            a.detection_ts,
            a.detection_host,
            a.detection_rule_name,
            a.detection_action            
        from
            global_temp.anomalies as a
            join {util.tagged_telemetry_table} as e 
            on a.detection_host = e.host_id
        where
            a.detection_action = 'temporal'
            and array_contains(map_values(e.sigma_pre_flux[a.detection_rule_name]), TRUE)
        """)

def start_query(args):
    create_spark_session("streaming temporal proximity alert builder", 1)

    # current time in milliseconds
    ts = int(time.time() * 1000)

    anomalies = (
        get_spark()
        .readStream
        .format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-delete-snapshots", True)
        .load(util.suspected_anomalies_table)
    )

    global_view("sigma_rule_to_action")

    def foreach_batch_function(anomalies, epoch_id):
        anomalies.persist()
        prox = find_temporal_proximity(anomalies)
        prox.persist()
        print_anomalies("context for historical temporal proximity:", prox)
        validated_prox = validate_events(prox)
        print_anomalies("validated historical temporal proximity:", validated_prox)
        store_alerts(validated_prox)
        get_spark().catalog.clearCache()
        anomalies.sparkSession.catalog.clearCache()



    streaming_query = (
        anomalies
        .writeStream
        .queryName("temporal_proximity")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(util.alerts_table) + "_temporal_proximity")
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

