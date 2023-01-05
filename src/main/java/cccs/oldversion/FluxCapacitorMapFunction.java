package cccs.oldversion;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class FluxCapacitorMapFunction implements FlatMapGroupsWithStateFunction<Integer, Row, BloomFilter, Row> {

  /*
   * 
   * 
   * I'm currently using a single Bloom filter. It will eventually fill up and
   * it's false positive rate will increase drastically.
   ** Future improvement**: We need to implement an "Age-Partitioned Bloom Filter".
   * 
   * The one I recommend is the forgetfull bloom filter. Quote from paper
   * https://arxiv.org/pdf/2001.03147.pdf
   * 
   * Forgetful Bloom Filter (FBF) [26], uses a future, a present and N ≥ 1 past
   * BFs. Inserts in the future and present components and queries (essentially)
   * by
   * testing the presence in two consecutive BFs from future to oldest past. When
   * full, the oldest past is discarded, other segments are shifted and a new
   * future is
   * added. The waste of space by duplicate insertion is somewhat compensated by
   * the reduced false-positive rate through the two-consecutive filters test.
   * However,
   * the correlation between consecutive segments caused by duplicate insertion
   * leads
   * to the false-positive rate reduction being modest and not space efficient.
   * 
   * 
   * I think there is a mistake in this quote. You should shift bloom filters when
   * the `future` bloom is half full. Not when it's full.
   * 
   * 
   */
  static Logger log = Logger.getLogger(FluxCapacitorMapFunction.class.getName());
  private final int bloom_capacity;
  private String specification;

  public FluxCapacitorMapFunction(int bloom_capacity, String specification) {
    this.bloom_capacity = bloom_capacity;
    this.specification = specification;
  }

  @Override
  public Iterator<Row> call(Integer groupKey, Iterator<Row> rows, GroupState<BloomFilter> state) throws Exception {
    long startTotal = System.currentTimeMillis();
    final BloomFilter<CharSequence> bloom;
    if (state.exists()) {
      bloom = (BloomFilter<CharSequence>) state.get();
    } else {
      bloom = BloomFilter.create(Funnels.stringFunnel(), bloom_capacity, 0.01);
    }

    long feature_set = 0;
    long feature_get = 0;

    Iterable<Row> rowIterable = () -> rows;
    List<Row> orderedRows = StreamSupport.stream(rowIterable.spliterator(), false).collect(Collectors.toList());
    sortInputRows(orderedRows);
    checkSorted(orderedRows);

    RulesConf spec = RulesConf.load(specification);
    List<Row> outputRows = orderedRows.stream()
        .map(row -> new RulesAdapter(new Rules(spec, bloom)).evaluateRow(row))
        .collect(Collectors.toList());

    // we can use this information to age off old blooms
    // bloomFilter.approximateElementCount();

    double fpp = bloom.expectedFpp();
    state.update(bloom);
    log.debug(outputRows.size() + " rows processed for group key: " + groupKey);
    log.debug("puts: " + feature_set + " gets: " + feature_get + " fpp: " + fpp);
    long endTotal = System.currentTimeMillis();
    log.debug("total time spent in map with group state (ms): " + (endTotal - startTotal));

    return outputRows.iterator();
  }

  private void sortInputRows(List<Row> orderedRows) {
    int timestampIndex = orderedRows.iterator().next().fieldIndex("timestamp");
    long startSort = System.currentTimeMillis();
    orderedRows.sort((Row r1, Row r2) -> r1.getTimestamp(timestampIndex).compareTo(r2.getTimestamp(timestampIndex)));
    long endSort = System.currentTimeMillis();
    log.debug("time to sort " + orderedRows.size() + " input rows (ms): " + (endSort - startSort));
  }

  private void checkSorted(List<Row> rows) {
    int timestampIndex = rows.iterator().next().fieldIndex("timestamp");
    Timestamp prev = null;
    Iterator<Row> it = rows.iterator();
    while (it.hasNext()) {
      Row row = it.next();
      Timestamp ts = row.getTimestamp(timestampIndex);
      if (prev == null) {
        prev = ts;
      } else if (prev.compareTo(ts) <= 0) {
        prev = ts;
      } else {
        log.error("!!!prev row " + prev + " preceeds current row " + ts);
        prev = ts;
      }
    }
  }

}
