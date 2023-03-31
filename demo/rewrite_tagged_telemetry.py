import sys
import time
from constants import init_argparse
import constants
from util import (
    get_spark,
    create_spark_session,
    
)

def start_rewrite(args):
    create_spark_session("rewrite tagged telemetry", num_machines=1, cpu_per_machine=30, shuffle_partitions=100)

    max_hour = "2222-01-01 00:00:00"

    sql = f"""
    CALL {constants.catalog}.system.rewrite_data_files(
            table => '{constants.tagged_telemetry_table}',
            strategy => 'binpack', 
            options => map('max-concurrent-file-group-rewrites', '30',
                           'partial-progress.enabled', 'true'),
            where => 'timestamp >= TIMESTAMP \\'1970-01-01 00:00:00\\'
                AND timestamp < TIMESTAMP \\'{max_hour}\\' '
        )
    """

    print(sql)
    get_spark().sql(sql).show()


def main() -> int:
    args = init_argparse()
    while True:
        start_rewrite(args)
        time.sleep(2 * 60 * 60)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

