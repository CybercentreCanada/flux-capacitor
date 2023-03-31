from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from jinja2 import Environment, FileSystemLoader, select_autoescape
import os
import time
import json
import constants


jinja_env = Environment(loader=FileSystemLoader("."), autoescape=select_autoescape())


def make_name(args, filename):
    basename = os.path.basename(filename)
    basename = basename.replace(".py", "")
    return f"{basename}__{args.schema}_t{args.trigger}"

def create_spark_session(name, num_machines, cpu_per_machine=15, shuffle_partitions=15):
    (
    SparkSession.builder.master(constants.master_uri)
    .appName(name)
    .config("spark.sql.shuffle.partitions", shuffle_partitions)
    .config("spark.executor.memory", "40g")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.cores", "1")
    .config("spark.executor.cores", cpu_per_machine)
    .config("spark.dynamicAllocation.enabled", False)
    .config("spark.cores.max", cpu_per_machine * num_machines)
    .config("spark.jars", "../target/flux-capacitor-1.jar")
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
    return fname

def render(name):
    template = jinja_env.get_template(fullPath(name))
    rendered_sql = template.render(constants.template_vars)
    return rendered_sql

def run(name) -> DataFrame:
    return get_spark().sql(render(name))

def create_dataframe(name) -> DataFrame:
    return get_spark().sql(render(name))


def create_view(name):
    df = run(name)
    df.createOrReplaceGlobalTempView(name)
    return df

def persisted_view(name):
    create_view(name).persist()

def drop(tname):
    get_spark().sql(f"drop table if exists {tname}")  
    
def read_flux_update_spec():
    with open("./templates/generated/flux_update_spec.yaml", "r") as f:
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

def get_checkpoint_location(table_name):
    parts = table_name.split(".")
    catalog_name = parts[0]
    schema_name = parts[1]
    table_name = parts[2]

    catalog_location = get_spark()._sc.getConf().get(f"spark.sql.catalog.{catalog_name}.warehouse")
    checkpoint_location = f"{catalog_location}/{schema_name}/{table_name}/checkpoint"
    return checkpoint_location

def store_alerts(df):
    df.writeTo(constants.alerts_table).append()

def store_tagged_telemetry(df):
    columns = list(constants.telemetry_schema.keys())
    columns.append("sigma_pre_flux")
    df.select(columns).writeTo(constants.tagged_telemetry_table).append()

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
    return get_spark().sql("""
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

def write_metrics(name, metric):
    os.makedirs("telemetry", exist_ok=True)
    with open(f"telemetry/{name}.log.json", "a") as f:
        f.write(json.dumps(metric) + '\n')

def monitor_query(query, name):
    batchId = -1
    while(True):
        time.sleep(1)
        if query.lastProgress:
            if batchId != query.lastProgress['batchId']:
                batchId = query.lastProgress['batchId']
                write_metrics(name, query.lastProgress)
