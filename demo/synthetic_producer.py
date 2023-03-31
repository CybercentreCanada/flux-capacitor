import constants
import time

from constants import init_argparse
from util import (
    make_name,
    monitor_query,
    get_checkpoint_location,
    create_spark_session,
    get_spark,
    create_dataframe,
)
import sys


def start_query(args):
    create_spark_session("streaming synthetic producer", 1)

    # current time in milliseconds
    ts = int(time.time() * 1000)

    (
        get_spark()
        .readStream.format("rate-micro-batch")
        .option("rowsPerBatch", 60 * 2000)
        .option("startTimestamp", ts)
        .load()
        .createOrReplaceTempView("rate_view")
    )

    df = create_dataframe("generate_synthetic_telemetry")

    streaming_query = (
        df.writeStream.format("iceberg")
        .queryName("synthetic producer")
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("path", constants.process_telemetry_table)
        .option("checkpointLocation", get_checkpoint_location(constants.process_telemetry_table))
        .start()
    )

    monitor_query(streaming_query, args.name)


def main() -> int:
    args = init_argparse()
    args.name = make_name(args, __file__)
    start_query(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
