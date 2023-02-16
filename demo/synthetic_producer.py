import util

from util import init_argparse, run, get_checkpoint_location, create_spark_session, get_spark
import sys

def start_query(args):
    create_spark_session("streaming synthetic producer", 1)

    (
        get_spark().readStream
        .format("rate")
        .option("rowsPerSecond", 5000)
        .load()
        .selectExpr('value', '(value+100000)/5000 as ts')
        .createOrReplaceTempView("rate_view")
    )

    df = run("generate_synthetic_telemetry")

    streaming_query = (
        df.writeStream
        .format("iceberg")
        .queryName("synthetic producer")
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger} seconds")
        .option("path", util.process_telemetry_table)
        .option("checkpointLocation", get_checkpoint_location(util.process_telemetry_table))
        .start()
    )

    streaming_query.awaitTermination()


def main() -> int:
    args = init_argparse()
    start_query(args)
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

