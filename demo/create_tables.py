import sys
import util
from util import create_spark_session, drop, init_argparse, run

def create_tables(args):
    create_spark_session("create tables", 1)

    drop(util.tagged_telemetry_table)
    drop(util.process_telemetry_table)
    drop(util.suspected_anomalies_table)
    drop(util.alerts_table)
    run("create_alert_table")
    run("create_tagged_telemetry_table")
    run("alter_tagged_telemetry_table")
    run("create_process_telemetry_table")
    #run("populate_process_telemetry_table")
    run("create_suspected_anomalies_table")
    # run("generate_synthetic_telemetry")


def main() -> int:
    args = init_argparse()
    create_tables(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())


