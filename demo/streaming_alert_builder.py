import sys
import time

from demo.constants import init_globals, parse_args
import demo.constants as constants
from demo.util import (
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

import logging as logging
log = logging.getLogger(__name__)

def find_parents(anomalies):
    parent_anomalies = anomalies.where("detection_action = 'parent'")
    parent_anomalies.createOrReplaceGlobalTempView("anomalies")
    if parent_anomalies.count() == 0:
        return parent_anomalies
    
    lookups = parent_anomalies.select('host_id', 'parent_id').collect()
    log.info(f"number of parents to lookup {len(lookups)}")

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
    log.info(rendered_sql)

    parents = get_spark().sql(rendered_sql)
    return parent_anomalies.unionAll(parents)


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
        log.info(f"number of parents to lookup {len(lookups)}")
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
        log.info(rendered_sql)
        
        parents = get_spark().sql(rendered_sql)
        parents.persist()

        all_ancestors = all_ancestors.unionAll(parents)
        all_ancestors.persist()

        has_more_parents_to_find = parents.where(parents.parent_id != '0').count() > 0
        log.info(f"has_more_parents_to_find: {has_more_parents_to_find}")
        childrens.unpersist(True)
        childrens = parents

    return all_ancestors

def find_temporal_proximity(anomalies):
    temporal_anomalies = anomalies.where("detection_action = 'temporal'")
    temporal_anomalies.createOrReplaceGlobalTempView("anomalies")
    num_temporal = temporal_anomalies.count()
    if num_temporal == 0:
        return temporal_anomalies

    lookups = temporal_anomalies.select('host_id').distinct().collect()
    log.info(f"number of temporal proxity anomalies to lookup {len(lookups)}", flush=True)

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
            join (
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
                    has_temporal_proximity_tags = TRUE
                    and host_id in (
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
        """
    rendered_sql = render_statement(statement, lookups=lookups)
    log.info(rendered_sql)
    return get_spark().sql(rendered_sql)

def force_caching_anomalies(anomalies, epoch_id):
    start = time.time()
    log.info(f"number of anomalies to process {anomalies.count()}")
    log.info(f"caching anomalies took {time.time() - start} seconds", flush=True)

def foreach_batch_parents(anomalies, epoch_id):
    log.info("START foreach_batch_parents", flush=True)
    start = time.time()
    # Transform and write batchDF
    parents = find_parents(anomalies)
    parents.persist()
    print_anomalies("context for historical parents:", parents)
    validated_parents = validate_events(parents)
    print_anomalies("validated historical parents:", validated_parents)
    run("insert_into_alerts")
    log.info(f"END foreach_batch_parents took {time.time() - start} seconds", flush=True)

def foreach_batch_ancestors(anomalies, epoch_id):
    log.info("START foreach_batch_ancestors", flush=True)
    start = time.time()
    # Transform and write batchDF
    ancestors = find_ancestors(anomalies)
    ancestors.persist()
    print_anomalies("context for historical ancestors:", ancestors)
    validated_ancestors = validate_events(ancestors)
    validated_ancestors.persist()
    print_anomalies("validated historical ancestors:", validated_ancestors)
    run("insert_into_alerts")
    log.info(f"END foreach_batch_ancestors took {time.time() - start} seconds", flush=True)

def foreach_batch_temporal_proximity(anomalies, epoch_id):
    log.info("START foreach_batch_temporal_proximity", flush=True)
    start = time.time()
    prox = find_temporal_proximity(anomalies)
    prox.persist()
    print_anomalies("context for historical temporal proximity:", prox)
    validated_prox = validate_events(prox)
    print_anomalies("validated historical temporal proximity:", validated_prox)
    run("insert_into_alerts")
    log.info(f"END foreach_batch_temporal_proximity took {time.time() - start} seconds", flush=True)

def start_query(catalog, schema, trigger, verbose):
    init_globals(catalog, schema, verbose)
    name = make_name(schema, trigger, __file__)
    create_spark_session("streaming alert builder", 1)

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
        anomalies.persist()
        force_caching_anomalies(anomalies, epoch_id)
        foreach_batch_parents(anomalies, epoch_id)
        foreach_batch_ancestors(anomalies, epoch_id)
        foreach_batch_temporal_proximity(anomalies, epoch_id)
        get_spark().catalog.clearCache()
        log.info(f"===========================================================================", flush=True)

    streaming_query = (
        anomalies
        .writeStream
        .queryName("alert builder")
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", get_checkpoint_location(constants.alerts_table) + "_alert_builder")
        .foreachBatch(foreach_batch_function)
        .start()
    )

    monitor_query(streaming_query, name)


def main() -> int:
    args = parse_args()
    start_query(args.catalog, args.schema, args.trigger, args.verbose)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

