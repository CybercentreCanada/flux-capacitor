from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from jinja2 import Environment, FileSystemLoader, select_autoescape
import argparse

master_uri = "spark://ver-1-spark-master-svc.spark:7077"
telemetry_schema = {
    'timestamp': 'timestamp',
    'host_id': 'string',
    'id': 'string',
    'parent_id': 'string',
    'captured_folder_colname': 'string',
    'Name': 'string',
    'ImagePath': 'string',
    'Commandline': 'string'
}
telemetry_columns = """
    timestamp,
    host_id,
    id,
    parent_id,
    captured_folder_colname,
    Name,
    ImagePath,
    Commandline
"""

jinja_env = Environment(loader=FileSystemLoader("."), autoescape=select_autoescape())

telemetry_schema = {
    'timestamp': 'timestamp',
    'host_id': 'string',
    'id': 'string',
    'parent_id': 'string',
    'captured_folder_colname': 'string',
    'Name': 'string',
    'ImagePath': 'string',
    'Commandline': 'string'
}
telemetry_columns = """
    timestamp,
    host_id,
    id,
    parent_id,
    captured_folder_colname,
    Name,
    ImagePath,
    Commandline
"""

catalog = ""
schema = ""
tagged_telemetry_table_only = ""
tagged_telemetry_table = ""

process_telemetry_table = ""
suspected_anomalies_table = ""
alerts_table : str = ""
template_vars : Dict[str, str] = {}


def init_argparse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="Description here"
    )
    parser.add_argument('--trigger', type=int, required=False, default=60)
    parser.add_argument('--catalog', type=str, required=True)
    parser.add_argument('--schema', type=str, required=True)

    args = parser.parse_args()
    
    global schema
    global catalog
    global tagged_telemetry_table_only
    
    global process_telemetry_table
    global tagged_telemetry_table
    global suspected_anomalies_table
    global alerts_table

    schema = args.schema
    catalog = args.catalog

    tagged_telemetry_table_only = "tagged_telemetry_table"
    tagged_telemetry_table = f"{catalog}.{schema}.{tagged_telemetry_table_only}"
    process_telemetry_table = f"{catalog}.{schema}.process_telemetry_table"
    suspected_anomalies_table = f"{catalog}.{schema}.suspected_anomalies"
    alerts_table = f"{catalog}.{schema}.alerts"

    global template_vars
    template_vars = {
        "suspected_anomalies_table": suspected_anomalies_table,
        "tagged_telemetry_table":tagged_telemetry_table,
        "process_telemetry_table": process_telemetry_table,
        "alerts_table": alerts_table,
        "telemetry_schema": telemetry_schema,
        "telemetry_columns": telemetry_columns
        }
    return args






def create_spark_session(name, num_machines, cpu_per_machine=15, shuffle_partitions=15):
    (
    SparkSession.builder.master(master_uri)
    .appName(name)
    .config("spark.sql.shuffle.partitions", shuffle_partitions)
    .config("spark.executor.memory", "40g")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.cores", "1")
    .config("spark.executor.cores", cpu_per_machine)
    .config("spark.dynamicAllocation.enabled", False)
    .config("spark.cores.max", cpu_per_machine * num_machines)
    .config("spark.jars", "../target/flux-capacitor-1.jar")
    .getOrCreate()
    )




def fullPath(name):
    return f"./templates/{name}.sql"

def render(name):
    template = jinja_env.get_template(fullPath(name))
    rendered_sql = template.render(template_vars)
    return rendered_sql

def run(name) -> DataFrame:
    return get_spark().sql(render(name))


def global_view(name):
    df = run(name)
    df.createOrReplaceGlobalTempView(name)
    return df

def persisted_view(name):
    global_view(name).persist()

def drop(tname):
    get_spark().sql(f"drop table if exists {tname}")  
    
def read_flux_update_spec():
    with open("./flux_update_spec.yaml", "r") as f:
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
    df.writeTo(alerts_table).append()

def store_tagged_telemetry(df):
    columns = list(telemetry_schema.keys())
    columns.append("sigma_pre_flux")
    df.select(columns).writeTo(tagged_telemetry_table).append()

def validate_events(df):
    df = df.withColumn("sigma", df.sigma_pre_flux).drop("sigma_final")
    flux_capacitor(df)
    (
        run("post_flux_eval_condition")
        .where("array_contains(sigma_final, detection_rule_name)")
        .selectExpr("detection_id")
        .createOrReplaceGlobalTempView("validated_detections")
    )
    return df.where("detection_id in (select * from global_temp.validated_detections)")
