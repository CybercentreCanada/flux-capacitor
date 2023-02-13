import time
from util import (
    find_ancestors,
    suspected_anomalies_table,
    get_checkpoint_location,
    get_spark,
    create_spark_session,
    alerts_table,
    global_view,
    print_anomalies,
    validate_events,
    store_alerts
)

create_spark_session("streaming ancestor alert builder", 1)

# current time in milliseconds
ts = int(time.time() * 1000)

anomalies = (
    get_spark()
    .readStream.format("iceberg")
    .option("stream-from-timestamp", ts)
    .option("streaming-skip-delete-snapshots", True)
    .load(suspected_anomalies_table)
)

global_view("sigma_rule_to_action")

def foreach_batch_function(anomalies, epoch_id):
    # Transform and write batchDF
    anomalies.persist()
    ancestors = find_ancestors(anomalies)
    ancestors.persist()
    print_anomalies("context for historical ancestors:", ancestors)
    validated_ancestors = validate_events(ancestors)
    print_anomalies("validated historical ancestors:", validated_ancestors)
    store_alerts(validated_ancestors)
    ancestors.unpersist(True)
    anomalies.unpersist(True)
    get_spark().catalog.clearCache()
    anomalies.sparkSession.catalog.clearCache()

streaming_query = (
    anomalies
    .writeStream
    .queryName("ancestors")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", get_checkpoint_location(alerts_table) + "_ancestors")
    .foreachBatch(foreach_batch_function)
    .start()
)

streaming_query.awaitTermination()
