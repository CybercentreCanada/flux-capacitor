import sys
from constants import init_argparse
import constants
from util import create_spark_session, drop, run


def create_tables(args):
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
    args = init_argparse()
    create_tables(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
