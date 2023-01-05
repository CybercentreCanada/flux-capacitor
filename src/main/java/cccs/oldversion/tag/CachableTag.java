package cccs.oldversion.tag;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import com.google.common.hash.BloomFilter;

import cccs.oldversion.RulesConf.RuleConf;

public abstract class CachableTag extends Tag {
  static Logger log = Logger.getLogger(CachableTag.class.getName());
  private String bloomGetKey;
  private String bloomPutKey;
  private boolean value;
  protected Row row;
  private final BloomFilter<CharSequence> bloom;
  protected final RuleConf ruleConf;
  protected final String tagname;

  public CachableTag(String tagname, RuleConf ruleConf, BloomFilter<CharSequence> bloom) {
    this.tagname = tagname;
    this.ruleConf = ruleConf;
    this.bloom = bloom;
  }

  @Override
  public final boolean evaluateRow(Row row) {
    this.row = row;
    bloomGetKey = makeBloomGetKey();
    bloomPutKey = makeBloomPutKey();
    return evaluate();
  }

  protected abstract boolean evaluate();
  
  protected abstract String makeBloomPutKey();

  protected abstract String makeBloomGetKey();

  private static String getColumnValue(String colname, Row row) {
		if (!colname.isEmpty()) {
			int i = row.fieldIndex(colname);
			if (row.isNullAt(i)) {
				return "";
			}
			return row.getString(i);
		} else {
			return "";
		}
	}

	protected static String makeBloomKey(String colname, String ruleName, String tagName, Row row) {
		String prefixValue = getColumnValue(colname, row);
    log.debug("row value retrieved for column: " + colname + " is: [" + prefixValue + "]");
		return ruleName + "." + prefixValue + "." + tagName;
	}

  public void storeInCache() {
    log.debug("storing key: " + bloomPutKey + " in bloom");
    bloom.put(bloomPutKey);
  }

  protected boolean isCached() {
    boolean ret = bloom.mightContain(bloomGetKey);
    if (ret) {
      log.debug("key: " + bloomGetKey + " found in bloom");
    } else {
      log.debug("key: " + bloomGetKey + " not found in bloom");
    }
    return ret;
  }

  @Override
  public void setCurrentValue(boolean value){
    reset(value);
  }

  private void reset(boolean value){
    this.value = value;
    this.bloomGetKey = null;
    this.bloomPutKey = null;
  }


  public boolean currentValue() {
    return value;
  }

  protected void mergeCachedWithCurrent() {
    value = currentValue() | isCached();
  }

  protected void pushOrPullFromCache() {
    if (currentValue()) {
      // put "this" tag in the bloom
      log.debug("the tag was raised in this row, store tag in bloom");
      storeInCache();
    } else {
      // get "this" tag from the bloom
      log.debug("the tag is not raised in this row, cached value is: " + isCached());
      mergeCachedWithCurrent();
      log.debug("merged value is " + currentValue());
    }
  }

}