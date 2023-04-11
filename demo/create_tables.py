import sys
from demo.constants import init_globals, parse_args
import demo.constants as constants
from demo.util import create_spark_session, drop, run


def create_tables(catalog, schema):
    init_globals(catalog, schema)
    create_spark_session("create tables", 1)
    drop(constants.tagged_telemetry_table)
    drop(constants.process_telemetry_table)
    drop(constants.suspected_anomalies_table)
    drop(constants.alerts_table)
    run("create_alert_table")
    run("create_tagged_telemetry_table")
    run("create_process_telemetry_table")
    run("create_suspected_anomalies_table")


def main() -> int:
    args = parse_args()
    create_tables(args.catalog, args.schema)
    return 0


if __name__ == "__main__":
    sys.exit(main())
