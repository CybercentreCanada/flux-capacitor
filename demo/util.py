from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from jinja2 import Environment, FileSystemLoader, select_autoescape
import os

master_uri = "spark://ver-1-spark-master-svc.spark:7077"

catalog = os.getenv("FLUX_DEMO_CATALOG")
if catalog is None:
    print("enviroment variable FLUX_DEMO_CATALOG is not specified")
    exit()

catalog = "hogwarts_users_u_ouo"
schema = "jccote"
tagged_telemetry_table_only = "tagged_telemetry_table"


tagged_telemetry_table = f"{catalog}.{schema}.{tagged_telemetry_table_only}"
process_telemetry_table = f"{catalog}.{schema}.process_telemetry_table"
suspected_anomalies_table = f"{catalog}.{schema}.suspected_anomalies"
alerts_table = f"{catalog}.{schema}.alerts"
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

template_vars = {
    "suspected_anomalies_table": suspected_anomalies_table,
    "tagged_telemetry_table":tagged_telemetry_table,
    "process_telemetry_table": process_telemetry_table,
    "alerts_table": alerts_table,
    "telemetry_schema": telemetry_schema,
    "telemetry_columns": telemetry_columns
    }

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
    bloom_capacity = 100000
    spark = get_spark()
    flux_stateful_function = spark._sc._jvm.cccs.fluxcapacitor.FluxCapacitor.invoke
    # distribute processing of each host by this key
    jdf = flux_stateful_function(input_df._jdf, "host_id", bloom_capacity, spec, True)
    df = DataFrame(jdf, spark)
    df.createOrReplaceGlobalTempView("flux_capacitor_output")
    return df

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
            join {tagged_telemetry_table} as e 
            on a.detection_host = e.host_id
        where
            a.detection_action = 'temporal'
            and array_contains(map_values(e.sigma_pre_flux[a.detection_rule_name]), TRUE)
        """)

def find_parents(anomalies):
    anomalies.createOrReplaceGlobalTempView("anomalies")
    parent_anomalies = anomalies.where("detection_action = 'parent'")
    if parent_anomalies.count() == 0:
        return parent_anomalies
    parents = get_spark().sql(f"""
        select
            e.*,
            a.detection_id,
            a.detection_ts,
            a.detection_host,
            a.detection_rule_name,
            a.detection_action
        from
            global_temp.anomalies as a
            join {tagged_telemetry_table} as e 
            on a.detection_host = e.host_id
            and a.parent_id = e.id
        where
            a.detection_action = 'parent'
        """)
    return parent_anomalies.unionAll(parents)

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
                join {tagged_telemetry_table} as e 
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

def print_anomalies(msg, df):
    print(msg)
    df.select('detection_action', 'detection_rule_name', 'timestamp', 'id', 'parent_id', 'Commandline').show(truncate=False)


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
    ).show(truncate=False)

def print_final_results(msg, df):
    print(msg)
    df.select(
        "id",
        "parent_id",
        "Commandline",
        "sigma_final.integration_test_parent",
        "sigma_final.integration_test_ancestor",
        "sigma_final.integration_test_temporal",
        "sigma_final.integration_test_temporal_ordered",
    ).show(truncate=False)

def create_spark_session(name, num_machines):
    (
    SparkSession.builder.master(master_uri)
    .appName(name)
    .config("spark.sql.shuffle.partitions", 15)
    .config("spark.executor.memory", "35g")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.cores", "2")
    .config("spark.executor.cores", 15)
    .config("spark.cores.max", 15 * num_machines)
    .config("spark.jars", "../target/flux-capacitor-1.jar")
    .getOrCreate()
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
        .where("sigma_final[detection_rule_name] = TRUE")
        .selectExpr("detection_id")
        .createOrReplaceGlobalTempView("validated_detections")
    )
    return df.where("detection_id in (select * from global_temp.validated_detections)")
