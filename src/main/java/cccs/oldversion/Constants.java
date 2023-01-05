package cccs.oldversion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Tables;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;

public class Constants {
    public static final boolean parquet = true;
    public static final String fileExt = (parquet ? ".parquet" : ".avro");
    public static final Configuration hadoopConf = new Configuration();
    public static final String destination = hadoopConf.get("table.location");
    public static final String tableLocation = Constants.destination
            + (parquet ? "parquet_events_table" : "avro_events_table");
    public static final Path tableLocationPath = new Path(tableLocation);
    public static final Tables tables = new HadoopTables(Constants.hadoopConf);

    public static final int widthMicros = 5 * 60 * 1000 * 1000;
    public static final Schema SCHEMA = new Schema(
            NestedField.required(1, "message_id", LongType.get()),
            NestedField.optional(2, "data", StringType.get()),
            NestedField.optional(3, "timestamp", TimestampType.withZone()),
            NestedField.optional(4, "timeperiod_loadedBy", LongType.get()),
            NestedField.optional(5, "message_body", BinaryType.get()));

    public static final PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA)
            .truncate("timeperiod_loadedBy", widthMicros)
            .build();

}
