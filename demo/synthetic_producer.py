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

import logging as logging
log = logging.getLogger(__name__)

def clear_checkpoints(dir):
    sc = get_spark()._sc
    hadoopConf = sc._jsc.hadoopConfiguration()
    FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    URI           = sc._gateway.jvm.java.net.URI

    fs = FileSystem.get(URI(dir), hadoopConf)
    exists = fs.exists(Path(dir))
    if exists:
        print(f"Deleting checkpoints folder {dir} in order to reset the timestamp generation")
        fs.delete(Path(dir), True)

def start_query(catalog, schema, trigger, verbose):
    init_globals(catalog, schema, verbose)
    name = make_name(schema, trigger, __file__)
    create_spark_session("streaming synthetic producer", 1)

    checkpoint_dir = get_checkpoint_location(constants.process_telemetry_table)
    clear_checkpoints(checkpoint_dir)

    # current time in milliseconds
    ts = int(time.time() * 1000)
    log.info(f"starting at time: {ts}, tigger at every {trigger} seconds and advancing {trigger * 1000} milliseconds per batch")
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
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    monitor_query(streaming_query, name)


def main() -> int:
    args = parse_args()
    start_query(args.catalog, args.schema, args.trigger, args.verbose)
    # we never expect to stop, if we do it indicates an error, return -1
    return -1


if __name__ == "__main__":
    sys.exit(main())
