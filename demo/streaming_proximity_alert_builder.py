import sys
import time
import jinja2

from constants import init_argparse
import constants
from util import (
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    create_view,
    make_name,
    monitor_query,
    print_anomalies,
    validate_events,
    store_alerts
)


def find_temporal_proximity(anomalies):
    temporal_anomalies = anomalies.where("detection_action = 'temporal'")
    temporal_anomalies.createOrReplaceGlobalTempView("anomalies")
    num_temporal = temporal_anomalies.count()
    if num_temporal == 0:
        return temporal_anomalies

    lookups = temporal_anomalies.select('host_id').distinct().collect()
    print(f"number of temporal proxity anomalies to lookup {len(lookups)}", flush=True)

    template = jinja2.Template("""
        select
            e.*,
            a.detection_id,
            a.detection_ts,
            a.detection_host,
            a.detection_rule_name,
            a.detection_action            
        from
            global_temp.anomalies as a
            join (
               select
                    * 
                from
                    {{tagged_telemetry_table}}
                where
                    host_id IN (
                    {%- for lookup in lookups -%}
                        '{{lookup.host_id}}'
                        {%- if not loop.last -%}
                            ,
                        {%- endif -%}
                    {%- endfor -%}
                    )
            ) as e 
            on a.detection_host = e.host_id
        where
            a.detection_action = 'temporal'
            and array_contains(map_values(e.sigma_pre_flux[a.detection_rule_name]), TRUE)
        """)
    rendered_sql = template.render({
        "lookups": lookups, 
        "tagged_telemetry_table": constants.tagged_telemetry_table})
    print(rendered_sql)
    return get_spark().sql(rendered_sql)

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
        .load(constants.suspected_anomalies_table)
    )

    create_view("sigma_rule_to_action")

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
        .option("checkpointLocation", get_checkpoint_location(constants.alerts_table) + "_temporal_proximity")
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

