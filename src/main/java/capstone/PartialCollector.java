package capstone;

import org.apache.parquet.Strings;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class PartialCollector extends UserDefinedAggregateFunction {
    private static final long serialVersionUID = 1333483630699465331L;

    @Override
    public StructType inputSchema() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("input", DataTypes.LongType, true));
        return DataTypes.createStructType(inputFields);
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("keys", DataTypes.StringType, true));
        return DataTypes.createStructType(inputFields);
    }

    @Override
    public DataType dataType() {
        return DataTypes.LongType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    private void aggregateAndCount(MutableAggregationBuffer buffer, Row input, final Boolean isMerge) {
        if (!input.isNullAt(0)) {
            final String alreadyPresents = buffer.getString(0);
            final String elem = isMerge ? input.getString(0) : Long.toString(input.getLong(0));
            if (!alreadyPresents.contains(elem)) {
                final String concat;
                if (!Strings.isNullOrEmpty(alreadyPresents))
                    concat = alreadyPresents.concat("," + elem);
                else
                    concat = elem;
                buffer.update(0, concat);
            }
        }
    }

    ;

    @Override
    public void update(final MutableAggregationBuffer buffer, final Row input) {
        aggregateAndCount(buffer, input, Boolean.FALSE);
    }

    @Override
    public void merge(final MutableAggregationBuffer buffer1, final Row buffer2) {
        aggregateAndCount(buffer1, buffer2, Boolean.TRUE);
    }

    @Override
    public Long evaluate(Row buffer) {
        final String concats = buffer.getString(0);
        return Arrays.stream(concats.split(",")).count();
    }
}
