from datetime import datetime
import sys
import time
import util
from util import (
    get_spark,
    init_argparse,
    create_spark_session,
    
)

def start_rewrite(args):
    create_spark_session("rewrite tagged telemetry", 2, cpu_per_machine=30, shuffle_partitions=100)

    # max_hour = get_spark().sql(f"""
    #     select
    #         date_trunc('HOUR', max(timestamp)) as hour
    #     from
    #         {util.tagged_telemetry_table}
    # """).collect()[0].hour

    # if max_hour is not None:
    #     print(max_hour)
    #     print(f"max time: {max_hour}")

    max_hour = "2222-01-01 00:00:00"

    sql = f"""
    CALL {util.catalog}.system.rewrite_data_files(
            table => '{util.schema}.{util.tagged_telemetry_table_only}',
            strategy => 'sort', 
            sort_order => 'host_id, id',
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
        time.sleep(60 * 60)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

