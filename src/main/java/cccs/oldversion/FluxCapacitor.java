package cccs.oldversion;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;

import com.google.common.hash.BloomFilter;

public class FluxCapacitor {

  private FluxCapacitor() {
  }

  public static Dataset<Row> invoke(Dataset<Row> df, String groupKey, int bloomCapacity, String specification) {

    /*
     * I did not spend much time on serialization. Just used java serialization.
     * 
     * Maybe not the best way to serialize, could use kyro?
     * 
     * The guava BloomFilter has methods to write/read itself into a I/O stream they
     * claim it's more compact.
     * Don't know how much?
     * 
     * If we used read/write approach what Encoder would we use? BinaryEncoder?
     */

    return df
        .groupByKey((MapFunction<Row, Integer>) row -> row.getAs(groupKey), Encoders.INT())
        .flatMapGroupsWithState(
            new FluxCapacitorMapFunction(bloomCapacity, specification),
            OutputMode.Append(),
            Encoders.javaSerialization(BloomFilter.class),
            RowEncoder.apply(df.schema()),
            GroupStateTimeout.NoTimeout());
  }
}