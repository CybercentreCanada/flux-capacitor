from typing import Dict, Any
import argparse

master_uri = "spark://ver-1-spark-master-svc.spark:7077"
telemetry_schema = {
    "timestamp": "timestamp",
    "host_id": "string",
    "id": "string",
    "parent_id": "string",
    "captured_folder_colname": "string",
    "Name": "string",
    "ImagePath": "string",
    "Commandline": "string",
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
tagged_telemetry_table = ""

process_telemetry_table = ""
suspected_anomalies_table = ""
alerts_table = ""
template_vars: Dict[str, Any] = {}


def init_argparse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(usage="%(prog)s [OPTION] [FILE]...", description="Description here")
    parser.add_argument("--trigger", type=int, required=False, default=60)
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)

    args = parser.parse_args()

    global schema
    global catalog

    global process_telemetry_table
    global tagged_telemetry_table
    global suspected_anomalies_table
    global alerts_table

    schema = args.schema
    catalog = args.catalog

    tagged_telemetry_table = f"{catalog}.{schema}.tagged_telemetry_table"
    process_telemetry_table = f"{catalog}.{schema}.process_telemetry_table"
    suspected_anomalies_table = f"{catalog}.{schema}.suspected_anomalies"
    alerts_table = f"{catalog}.{schema}.alerts"

    global template_vars
    template_vars = {
        "suspected_anomalies_table": suspected_anomalies_table,
        "tagged_telemetry_table": tagged_telemetry_table,
        "process_telemetry_table": process_telemetry_table,
        "alerts_table": alerts_table,
        "telemetry_schema": telemetry_schema,
        "telemetry_columns": telemetry_columns,
    }
    return args
