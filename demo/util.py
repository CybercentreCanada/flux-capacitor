from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template
import os
import time
import json
import demo.constants as constants
from pyspark.sql.types import StructType

import logging as logging
log = logging.getLogger(__name__)

demo_dir = os.path.dirname(__file__)
template_dirs = [demo_dir + "/templates/static", demo_dir + "/templates/generated"]
jinja_env = Environment(loader=FileSystemLoader(template_dirs),autoescape=select_autoescape())
log.info(f"jinja templates load from {template_dirs}")

def make_name(schema, trigger, filename):
    basename = os.path.basename(filename)
    basename = basename.replace(".py", "")
    return f"{basename}__{schema}_t{trigger}"

def create_spark_session(name, num_machines, cpu_per_machine=15, shuffle_partitions=15, driver_mem="2g", use_kyro=True):
    demo_dir = os.path.dirname(__file__)

    builder = (
    SparkSession.builder.master(constants.master_uri)
    .appName(name)
    .config("spark.sql.shuffle.partitions", shuffle_partitions)
    .config("spark.executor.memory", "40g")
    .config("spark.driver.memory", driver_mem)
    .config("spark.driver.cores", "1")
    .config("spark.executor.cores", cpu_per_machine)
    .config("spark.dynamicAllocation.enabled", False)
    .config("spark.cores.max", cpu_per_machine * num_machines)
    .config("spark.jars", f"{demo_dir}/flux-capacitor.jar")
    .config("spark.sql.streaming.stateStore.providerClass", 
        "org.apache.spark.sql.execution.streaming.state.FluxStateStoreProvider")
    .config("spark.sql.streaming.maxBatchesToRetainInMemory", 1)
    #.config("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
    #.config("spark.driver.extraJavaOptions", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4747")
    )
    if use_kyro:
        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder.getOrCreate()

def render_statement(statement, **kwargs):
    kwargs.update(constants.template_vars)
    template = Template(statement)
    rendered_sql = template.render(kwargs)
    return rendered_sql

def render_file(name, **kwargs):
    kwargs.update(constants.template_vars)
    template = jinja_env.get_template(name + ".sql")
    rendered_sql = template.render(kwargs)
    return rendered_sql

def run(name) -> DataFrame:
    return get_spark().sql(render_file(name))

def create_dataframe(name) -> DataFrame:
    return get_spark().sql(render_file(name))


def create_view(name):
    df = run(name)
    df.createOrReplaceGlobalTempView(name)
    return df

def persisted_view(name):
    create_view(name).persist()

def drop(tname):
    get_spark().sql(f"drop table if exists {tname}")  
    
def read_flux_update_spec():
    demo_dir = os.path.dirname(__file__)

    with open(f"{demo_dir}/templates/generated/flux_update_spec.yaml", "r") as f:
        flux_update_spec = f.read()
    return flux_update_spec

def flux_capacitor(input_df):
    """Python function to invoke the scala code"""
    spec = read_flux_update_spec()
    bloom_capacity = 200000
    spark = get_spark()
    flux_stateful_function = spark._sc._jvm.cccs.fluxcapacitor.FluxCapacitor.invoke
    # distribute processing of each host by this key
    jdf = flux_stateful_function(input_df._jdf, "host_id", bloom_capacity, spec, True)
    df = DataFrame(jdf, spark)
    df.createOrReplaceGlobalTempView("flux_capacitor_output")
    return df

def print_telemetry(msg, df):
    if log.isEnabledFor(logging.INFO):
        log.info(msg)
        df.select(
            "id",
            "parent_id",
            "Commandline",
            "sigma.integration_test_parent",
            "sigma.integration_test_ancestor",
            "sigma.integration_test_temporal",
            "sigma.integration_test_temporal_ordered",
        ).orderBy("timestamp").show(truncate=False)


def print_anomalies(msg, df):
    if log.isEnabledFor(logging.INFO):
        log.info(msg)
        n = df.count()
        log.info(f"table contains {n} rows")
        (df.select('detection_action', 'detection_rule_name', 'timestamp', 'id', 'parent_id', 'Commandline')
        .orderBy("timestamp")
        .show(truncate=False)
        )

def print_final_results(msg, df):
    if log.isEnabledFor(logging.INFO):
        log.info(msg)
        (df.select(
            "id",
            "parent_id",
            "Commandline",
            "sigma_final",
        )
        .orderBy("timestamp")
        .show(truncate=False)
        )

def get_spark():
    return SparkSession.getActiveSession()

def get_table_location(table_name):
    # when describe table is called the result is a table of two columns named col_name and data_type.
    # The row with a col_name = Location is the value we are seeking
    log.info(f"describe extended {table_name}")
    rows = get_spark().sql(f"describe extended {table_name}").where("col_name = 'Location'").collect()
    location = rows[0].data_type
    return location

def get_data_location(table_name):
    location = get_table_location(table_name)
    return f"{location}/data"

def get_metadata_location(table_name):
    location = get_table_location(table_name)
    return f"{location}/metadata"

def get_checkpoint_location(table_name):
    location = get_table_location(table_name)
    return f"{location}/checkpoint"

def validate_events(df):
    # the incoming dataframe contains the event rows to validate
    # we skip re-evaluating the pre-flux patterns and instead use the stored sigma_pre_flux tags
    df = df.withColumn("sigma", df.sigma_pre_flux).drop("sigma_final")
    df.createOrReplaceGlobalTempView("events_to_validate")
    # we re-apply time travel
    flux_capacitor(df)
    # and re-apply the post-flux conditions
    #post_df = 
    persisted_view("post_flux_eval_condition")
    detection_count = get_spark().sql("""
        select
            detection_id
        from
            global_temp.post_flux_eval_condition
        where
            array_contains(sigma_final, detection_rule_name)
        """).count()
    log.info(f"number of detection IDs which are validated {detection_count}")
    # The incomming df contains a detection_id column
    # determine which detection_id are validated
    # and use this list of validated detection ids to filer the incoming event rows
    validated_events = get_spark().sql("""
        select
            *
        from
            global_temp.events_to_validate
        where
            detection_id in (
                select
                    detection_id
                from
                    global_temp.post_flux_eval_condition
                where
                    array_contains(sigma_final, detection_rule_name)
            )
    """)
    validated_events.createOrReplaceGlobalTempView("validated_events")
    return validated_events

def write_metrics(name, metric):
    json_string = json.dumps(metric)
    schema = get_spark().table(constants.metrics_table).schema
    df = get_spark().read.json(get_spark()._sc.parallelize([json_string]), schema=schema)
    df.write.format("iceberg").insertInto(constants.metrics_table)

def monitor_query(query, name):
    batchId = -1
    sleep_time = 0
    while(True):
        time.sleep(10)
        if query.lastProgress:
            if batchId != query.lastProgress['batchId']:
                batchId = query.lastProgress['batchId']
                log.debug(query.status)
                write_metrics(name, query.lastProgress)
                sleep_time = 0
        if query.exception():
            log.error(query.exception)
            raise Exception(f"Streaming query {name} failed with query exception {query.exception}")
        sleep_time = sleep_time + 10
        if sleep_time % 600 == 0:
            log.error(f"Sleept for {sleep_time} waiting for trigger to report progress. This should not happen.")
