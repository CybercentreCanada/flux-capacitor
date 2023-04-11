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
    run
)

def find_ancestors(anomalies):      
    ancestor_anomalies = anomalies.where("detection_action = 'ancestor'")
    if ancestor_anomalies.count() == 0:
        return ancestor_anomalies

    childrens = ancestor_anomalies
    all_ancestors = childrens

    has_more_parents_to_find = True
    while has_more_parents_to_find:
        childrens.createOrReplaceGlobalTempView("childrens")
        lookups = childrens.select('host_id', 'parent_id').collect()
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
                global_temp.childrens as a
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
                        {% for lookup in lookups -%}
                            ( host_id = '{{lookup.host_id}}' and id = '{{lookup.parent_id}}' )
                            {% if not loop.last -%}
                                or
                            {% endif -%}
                        {%- endfor -%}
                ) as e 
                on a.detection_host = e.host_id
                and a.parent_id = e.id
            where
                a.detection_action = 'ancestor'
            """
        rendered_sql = render_statement(statement, lookups=lookups)
        print(rendered_sql)
        
        parents = get_spark().sql(rendered_sql)
        parents.persist()

        all_ancestors = all_ancestors.unionAll(parents)
        all_ancestors.persist()

        has_more_parents_to_find = parents.where(parents.parent_id != '0').count() > 0
        print(f"has_more_parents_to_find: {has_more_parents_to_find}")
        childrens.unpersist(True)
        childrens = parents

    return all_ancestors


def start_query(args):
    create_spark_session("streaming ancestor alert builder", 1, cpu_per_machine=30)

    # current time in milliseconds
    ts = int(time.time() * 1000)

    anomalies = (
        get_spark()
        .readStream.format("iceberg")
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
        ancestors = find_ancestors(anomalies)
        ancestors.persist()
        print_anomalies("context for historical ancestors:", ancestors)
        validated_ancestors = validate_events(ancestors)
        validated_ancestors.persist()
        print_anomalies("validated historical ancestors:", validated_ancestors)
        run("insert_into_alerts")
        #ancestors.unpersist(True)
        #anomalies.unpersist(True)
        get_spark().catalog.clearCache()
        #anomalies.sparkSession.catalog.clearCache()
        print("END", flush=True)

    streaming_query = (
        anomalies
        .writeStream
        .queryName("ancestors")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(constants.alerts_table) + "_ancestors")
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

