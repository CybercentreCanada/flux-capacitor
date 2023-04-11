import sys
import time
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
    render_statement,
    validate_events,
    run,
    
)

def find_parents(anomalies):
    parent_anomalies = anomalies.where("detection_action = 'parent'")
    parent_anomalies.createOrReplaceGlobalTempView("anomalies")
    if parent_anomalies.count() == 0:
        return parent_anomalies
    
    lookups = parent_anomalies.select('host_id', 'parent_id').collect()
    print(f"number of parents to lookup {len(lookups)}")

    statement = """
        select
            e.*,
            a.detection_id,
            a.detection_ts,
            a.detection_host,
            a.detection_rule_name,
            a.detection_action
        from
            global_temp.anomalies as a
            join
            (
               select
                    /* include all the columns of the original telemetry */
                    {% for column_name, column_type in telemetry_schema.items() -%}
                        {{column_name}},
                    {% endfor -%}
                    /* add tests performed by pre-flux statement */
                    sigma_pre_flux
                from
                    {{tagged_telemetry_table}}
                where
                    {%- for lookup in lookups %}
                        ( host_id = '{{lookup.host_id}}' and id = '{{lookup.parent_id}}' )
                        {% if not loop.last %}
                            or
                        {% endif %}
                    {% endfor %}
            ) as e 
            on a.detection_host = e.host_id
            and a.parent_id = e.id
        """
    rendered_sql = render_statement(statement, lookups=lookups)
    print(rendered_sql)

    parents = get_spark().sql(rendered_sql)
    return parent_anomalies.unionAll(parents)


def start_query(args):
    create_spark_session("streaming parent alert builder", 1)

    # current time in milliseconds
    ts = int(time.time() * 1000)

    anomalies = (
        get_spark()
        .readStream
        .format("iceberg")
        .option("stream-from-timestamp", ts)
        .option("streaming-skip-overwrite-snapshots", True)
        .option("streaming-skip-delete-snapshots", True)
        .load(constants.suspected_anomalies_table)
    )

    create_view("sigma_rule_to_action")

    def foreach_batch_function(anomalies, epoch_id):
        print("START", flush=True)
        # Transform and write batchDF
        anomalies.persist()
        parents = find_parents(anomalies)
        parents.persist()
        print_anomalies("context for historical parents:", parents)
        validated_parents = validate_events(parents)
        print_anomalies("validated historical parents:", validated_parents)
        run("insert_into_alerts")
        # parents.unpersist(True)
        # anomalies.unpersist(True)
        get_spark().catalog.clearCache()
        # anomalies.sparkSession.catalog.clearCache()
        print("END", flush=True)

    streaming_query = (
        anomalies
        .writeStream
        .queryName("parents")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(constants.alerts_table) + "_parents")
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

