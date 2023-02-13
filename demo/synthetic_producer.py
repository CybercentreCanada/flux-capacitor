from util import run, process_telemetry_table, get_checkpoint_location, create_spark_session, get_spark

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
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("path", process_telemetry_table)
    .option("checkpointLocation", get_checkpoint_location(process_telemetry_table))
    .start()
)

streaming_query.awaitTermination()