package cccs.oldversion.tag;

import org.apache.spark.sql.Row;

public class TransitoryTag extends Tag {

    public static TransitoryTag TRUE = new TransitoryTag(true);
    public static TransitoryTag FALSE = new TransitoryTag(false);
    private final boolean value;

    private TransitoryTag(boolean value) {
        this.value = value;
    }

    public static TransitoryTag of(boolean value) {
        if (value) {
            return TRUE;
        }
        return FALSE;
    }

    @Override
    public boolean currentValue() {
        return value;
    }

    @Override
    public boolean evaluateRow(Row row) {
        log.debug("evaluating ImmutableTag");
        return currentValue();
    }

    @Override
    public void setCurrentValue(boolean value) {        
    }
}
