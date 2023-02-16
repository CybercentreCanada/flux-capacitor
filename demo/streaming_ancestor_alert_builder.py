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


def find_ancestors(anomalies):      
    ancestor_anomalies = anomalies.where("detection_action = 'ancestor'")
    if ancestor_anomalies.count() == 0:
        return ancestor_anomalies

    childrens = ancestor_anomalies
    all_ancestors = childrens

    has_more_parents_to_find = True
    while has_more_parents_to_find > 0:
        childrens.createOrReplaceGlobalTempView("childrens")
        parents = get_spark().sql(f"""
            select
                e.*,
                a.detection_id,
                a.detection_ts,
                a.detection_host,
                a.detection_rule_name,
                a.detection_action                
            from
                global_temp.childrens as a
                join {util.tagged_telemetry_table} as e 
                on a.detection_host = e.host_id
                and a.parent_id = e.id
            where
                a.detection_action = 'ancestor'
            """)

        parents.persist()

        all_ancestors = all_ancestors.unionAll(parents)
        all_ancestors.persist()

        has_more_parents_to_find = parents.where(parents.parent_id != '0').count() > 0
        print(f"has_more_parents_to_find: {has_more_parents_to_find}")
        childrens.unpersist(True)
        childrens = parents

    return all_ancestors

def start_query(args):
    create_spark_session("streaming ancestor alert builder", 1)

    # current time in milliseconds
    ts = int(time.time() * 1000)

    anomalies = (
        get_spark()
        .readStream.format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-delete-snapshots", True)
        .load(util.suspected_anomalies_table)
    )

    global_view("sigma_rule_to_action")

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

    streaming_query = (
        anomalies
        .writeStream
        .queryName("ancestors")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(util.alerts_table) + "_ancestors")
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

