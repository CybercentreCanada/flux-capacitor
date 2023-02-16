from datetime import datetime
import sys
from experiment_agg_bloom import init_argparse
import util
from util import (
    get_spark,
    create_spark_session,
    
)

def start_rewrite(args):
    create_spark_session("rewrite tagged telemetry", 1)

    max_partition = get_spark().sql(f"""
        select
            max(partition.timestamp_hour) as hour
        from
            {util.tagged_telemetry_table}.files
    """).take(1)[0].hour

    print(f"max partition hour: {max_partition}")

    for h in range(0, max_partition):
        partition = datetime.fromtimestamp(h * 60 * 60).strftime("%Y-%m-%d %H:%M:%S")
        print(partition)
        sql = f"""
        CALL {util.catalog}.system.rewrite_data_files(
                table => '{util.schema}.{util.tagged_telemetry_table_only}',
                strategy => 'sort', 
                sort_order => 'host_id',
                where => 'timestamp >= TIMESTAMP \\'{partition}\\'
                    AND timestamp < TIMESTAMP \\'{partition}\\' + INTERVAL 1 HOUR'
            )
        """

        print(sql)
        get_spark().sql(sql).show()


def main() -> int:
    args = init_argparse()
    start_rewrite(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

