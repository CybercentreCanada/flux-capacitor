from typing import Dict, Any
import argparse
import logging as logging

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

verbose = False
catalog = ""
schema = ""
tagged_telemetry_table = ""

metrics_table = ""
process_telemetry_table = ""
suspected_anomalies_table = ""
alerts_table = ""
template_vars: Dict[str, Any] = {}

log = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(usage="%(prog)s [OPTION] [FILE]...", description="Description here")
    parser.add_argument("--trigger", type=int, required=False, default=60)
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--schema", type=str, required=True)
    parser.add_argument("--verbose", type=bool, required=False)
    args = parser.parse_args()
    return args

def init_globals(the_catalog, the_schema, the_verbose):
    print(f"running in verbose mode {the_verbose}")
    if the_verbose:
        logging.basicConfig(level=logging.INFO, force=True)
    else:
        logging.basicConfig(level=logging.WARN, force=True)

    log.info(f"catalog={the_catalog}, schema={the_schema}")

    global verbose
    global schema
    global catalog

    global process_telemetry_table
    global tagged_telemetry_table
    global suspected_anomalies_table
    global alerts_table
    global metrics_table

    schema = the_schema
    catalog = the_catalog
    verbose = the_verbose

    tagged_telemetry_table = f"{catalog}.{schema}.tagged_telemetry_table"
    process_telemetry_table = f"{catalog}.{schema}.process_telemetry_table"
    suspected_anomalies_table = f"{catalog}.{schema}.suspected_anomalies"
    alerts_table = f"{catalog}.{schema}.alerts"
    metrics_table = f"{catalog}.{schema}.metrics_table"

    global template_vars
    template_vars = {
        "suspected_anomalies_table": suspected_anomalies_table,
        "tagged_telemetry_table": tagged_telemetry_table,
        "process_telemetry_table": process_telemetry_table,
        "alerts_table": alerts_table,
        "telemetry_schema": telemetry_schema,
        "metrics_table": metrics_table,
    }
