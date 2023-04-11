from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template
import os
import time
import json
import demo.constants as constants

demo_dir = os.path.dirname(__file__)

jinja_env = Environment(loader=FileSystemLoader(demo_dir), autoescape=select_autoescape())
print(f"jinja templates load from {demo_dir}")

def make_name(schema, trigger, filename):
    basename = os.path.basename(filename)
    basename = basename.replace(".py", "")
    return f"{basename}__{schema}_t{trigger}"

def create_spark_session(name, num_machines, cpu_per_machine=15, shuffle_partitions=15, driver_mem="2g"):
    demo_dir = os.path.dirname(__file__)

    (
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
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
    )

def fullPath(name):
    fname = f"./templates/static/{name}.sql"
    if os.path.isfile(fname): 
        return fname
    fname = f"./templates/generated/{name}.sql"
    if os.path.isfile(fname): 
        return fname
    raise Exception(f"Can't find template file {name} if either ./templates/static or ./templates/generated")

def render_statement(statement, **kwargs):
    kwargs.update(constants.template_vars)
    template = Template(statement)
    rendered_sql = template.render(kwargs)
    return rendered_sql

def render_file(name, **kwargs):
    kwargs.update(constants.template_vars)
    template = jinja_env.get_template(fullPath(name))
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
    print(msg)
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
    print(msg)
    (df.select('detection_action', 'detection_rule_name', 'timestamp', 'id', 'parent_id', 'Commandline')
    .orderBy("timestamp")
    .show(truncate=False)
    )

def print_final_results(msg, df):
    print(msg)
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

# def store_alerts(df):
#     df.writeTo(constants.alerts_table).append()

# def store_tagged_telemetry(df):
#     columns = list(constants.telemetry_schema.keys())
#     columns.append("sigma_pre_flux")
#     columns.append("has_temporal_proximity_tags")
#     df.select(columns).writeTo(constants.tagged_telemetry_table).append()

def validate_events(df):
    # the incoming dataframe contains the event rows to validate
    # we skip re-evaluating the pre-flux patterns and instead use the stored sigma_pre_flux tags
    df = df.withColumn("sigma", df.sigma_pre_flux).drop("sigma_final")
    df.createOrReplaceGlobalTempView("events_to_validate")
    # we re-apply time travel
    flux_capacitor(df)
    # and re-apply the post-flux conditions
    #post_df = 
    create_view("post_flux_eval_condition")
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
    demo_dir = os.path.dirname(__file__)
    os.makedirs(f"{demo_dir}/telemetry", exist_ok=True)
    with open(f"{demo_dir}/telemetry/{name}.log.json", "a") as f:
        f.write(json.dumps(metric) + '\n')

def monitor_query(query, name):
    batchId = -1
    while(True):
        time.sleep(10)
        print(query.status)
        if query.lastProgress:
            if batchId != query.lastProgress['batchId']:
                batchId = query.lastProgress['batchId']
                write_metrics(name, query.lastProgress)
        if query.exception():
            print(query.exception, flush=True)
            exit(-1)
