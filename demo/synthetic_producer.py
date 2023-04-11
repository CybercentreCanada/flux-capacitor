import time
import sys

import demo.constants as constants
from demo.constants import init_globals, parse_args
from demo.util import (
    make_name,
    monitor_query,
    get_checkpoint_location,
    create_spark_session,
    get_spark,
    create_dataframe,
)


def start_query(catalog, schema, trigger):
    init_globals(catalog, schema)
    name = make_name(schema, trigger, __file__)
    create_spark_session("streaming synthetic producer", 1)

    # current time in milliseconds
    ts = int(time.time() * 1000)
    print(f"starting at time: {ts}, tigger at every {trigger} seconds and advancing {trigger * 1000} milliseconds per batch", flush=True)
    (
        get_spark()
        .readStream.format("rate-micro-batch")
        .option("rowsPerBatch", 60 * 5000)
        .option("startTimestamp", ts)
        .option("advanceMillisPerBatch", trigger * 1000)
        .load()
        .createOrReplaceTempView("rate_view")
    )

    df = create_dataframe("generate_synthetic_telemetry")

    streaming_query = (
        df.writeStream.format("iceberg")
        .queryName("synthetic producer")
        .outputMode("append")
        .trigger(processingTime=f"{trigger} seconds")
        .option("path", constants.process_telemetry_table)
        .option("checkpointLocation", get_checkpoint_location(constants.process_telemetry_table))
        .start()
    )

    monitor_query(streaming_query, name)


def main() -> int:
    args = parse_args()
    start_query(args.catalog, args.schema, args.trigger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
