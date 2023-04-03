import sys
import time
from constants import init_argparse
import constants
from util import (
    get_spark,
    create_spark_session,
    
)

def start_rewrite(args):
    create_spark_session("rewrite tagged telemetry", num_machines=1)

    min_ts = "2020-01-01 00:00:00"
    max_ts = "2028-01-01 00:00:00"

    sql = f"""
    CALL {constants.catalog}.system.rewrite_data_files(
            table => '{constants.tagged_telemetry_table}',
            strategy => 'sort',
            sort_order => 'host_id',
            options => map('min-input-files', '100',
                           'max-concurrent-file-group-rewrites', '30',
                           'partial-progress.enabled', 'true'),
            where => 'timestamp >= TIMESTAMP \\'{min_ts}\\' AND timestamp < TIMESTAMP \\'{max_ts}\\' '
        )
    """

    print(sql)
    get_spark().sql(sql).show()


def main() -> int:
    args = init_argparse()
    start_rewrite(args)

    # calling the store procedure in a loop causes it to not detect files to compact
    # if you re-start the spark job then it works, seems like there is a bug in iceberg's
    # procedure?
    
    # while True:
    #     start_rewrite(args)
    #     print(f"sleeping for {args.trigger}", flush=True)
    #     time.sleep(args.trigger)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

