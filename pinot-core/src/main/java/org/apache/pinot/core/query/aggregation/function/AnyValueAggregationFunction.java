package org.apache.pinot.core.query.aggregation.function;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.AnyValueObject;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class AnyValueAggregationFunction extends BaseSingleInputAggregationFunction<AnyValueObject, AnyValueObject> {
  private final boolean _nullHandlingEnabled;

  private final ExpressionContext _anyValueColumn;

  private final ThreadLocal<DataSchema> _anyValueColumnSchema = new ThreadLocal<>();
  // If the schemas are initialized
  private final ThreadLocal<Boolean> _schemaInitialized = ThreadLocal.withInitial(() -> false);

  public AnyValueAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "ANY_VALUE"), nullHandlingEnabled);
  }

  protected AnyValueAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
    _anyValueColumn = expression;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ANYVALUE;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    AnyValueObject anyValueObject = aggregationResultHolder.getResult();
    // No need to look at any other values once we choose a value to use
    if (anyValueObject != null) {
      return;
    }

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    initializeColumnSchema(blockValSet);

    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap == null) {
        nullBitmap = new RoaringBitmap();
      }
      aggregateNullHandlingEnabled(length, aggregationResultHolder, blockValSet, nullBitmap);
      return;
    }

    anyValueObject = new AnyValueObject(_anyValueColumnSchema.get());
    if (blockValSet.isSingleValue()) {
      switch (blockValSet.getValueType().getStoredType()) {
        case INT: {
          int[] values = blockValSet.getIntValuesSV();
          int val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case LONG: {
          long[] values = blockValSet.getLongValuesSV();
          long val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case FLOAT: {
          float[] values = blockValSet.getFloatValuesSV();
          float val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case DOUBLE: {
          double[] values = blockValSet.getDoubleValuesSV();
          double val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case BIG_DECIMAL: {
          BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
          BigDecimal val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case STRING: {
          String[] values = blockValSet.getStringValuesSV();
          String val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case BYTES: {
          byte[][] values = blockValSet.getBytesValuesSV();
          byte[] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        default:
          throw new IllegalStateException("Cannot compute SV ANY_VALUE for type: " + blockValSet.getValueType());
      }
    } else {
      switch (blockValSet.getValueType().getStoredType()) {
        case INT: {
          int[][] values = blockValSet.getIntValuesMV();
          int[] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case LONG: {
          long[][] values = blockValSet.getLongValuesMV();
          long[] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case FLOAT: {
          float[][] values = blockValSet.getFloatValuesMV();
          float[] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case DOUBLE: {
          double[][] values = blockValSet.getDoubleValuesMV();
          double[] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case STRING: {
          String[][] values = blockValSet.getStringValuesMV();
          String[] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        case BYTES: {
          byte[][][] values = blockValSet.getBytesValuesMV();
          byte[][] val = values[0];
          anyValueObject.setValueIfNeeded(val);
          break;
        }
        default:
          throw new IllegalStateException("Cannot compute MV ANY_VALUE for type: " + blockValSet.getValueType());
      }
    }
    aggregationResultHolder.setValue(anyValueObject);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    initializeColumnSchema(blockValSet);

    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap == null) {
        nullBitmap = new RoaringBitmap();
      }
      aggregateGroupByNullHandlingEnabled(length, groupKeyArray, groupByResultHolder, blockValSet, nullBitmap);
      return;
    }

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      AnyValueObject anyValueObject = groupByResultHolder.getResult(groupKey);
      if (anyValueObject != null) {
        continue;
      }

      anyValueObject = new AnyValueObject(_anyValueColumnSchema.get());
      switch (blockValSet.getValueType().getStoredType()) {
        case INT: {
          int[] valueArray = blockValSet.getIntValuesSV();
          int value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        case LONG: {
          long[] valueArray = blockValSet.getLongValuesSV();
          long value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        case FLOAT: {
          float[] valueArray = blockValSet.getFloatValuesSV();
          float value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        case DOUBLE: {
          double[] valueArray = blockValSet.getDoubleValuesSV();
          double value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        case BIG_DECIMAL: {
          BigDecimal[] valueArray = blockValSet.getBigDecimalValuesSV();
          BigDecimal value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        case STRING: {
          String[] valueArray = blockValSet.getStringValuesSV();
          String value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        case BYTES: {
          byte[][] valueArray = blockValSet.getBytesValuesSV();
          byte[] value = valueArray[i];
          anyValueObject.setValueIfNeeded(value);
          break;
        }
        default:
          throw new IllegalStateException("Cannot compute SV ANY_VALUE for type: " + blockValSet.getValueType());
      }
      groupByResultHolder.setValueForKey(groupKey, anyValueObject);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    // TODO: Fix this up when testing MV. Do we return full MV row or a single value in that row?
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    initializeColumnSchema(blockValSet);

    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        AnyValueObject anyValueObject = groupByResultHolder.getResult(groupKey);
        if (anyValueObject != null) {
          continue;
        }

        anyValueObject = new AnyValueObject(_anyValueColumnSchema.get());
        switch (blockValSet.getValueType()) {
          case INT: {
            int[][] valueArray = blockValSet.getIntValuesMV();
            int[] value = valueArray[i];
            anyValueObject.setValueIfNeeded(value);
            break;
          }
          case LONG: {
            long[][] valueArray = blockValSet.getLongValuesMV();
            long[] value = valueArray[i];
            anyValueObject.setValueIfNeeded(value);
            break;
          }
          case FLOAT: {
            float[][] valueArray = blockValSet.getFloatValuesMV();
            float[] value = valueArray[i];
            anyValueObject.setValueIfNeeded(value);
            break;
          }
          case DOUBLE: {
            double[][] valueArray = blockValSet.getDoubleValuesMV();
            double[] value = valueArray[i];
            anyValueObject.setValueIfNeeded(value);
            break;
          }
          case STRING: {
            String[][] valueArray = blockValSet.getStringValuesMV();
            String[] value = valueArray[i];
            anyValueObject.setValueIfNeeded(value);
            break;
          }
          case BYTES: {
            byte[][][] valueArray = blockValSet.getBytesValuesMV();
            byte[][] value = valueArray[i];
            anyValueObject.setValueIfNeeded(value);
            break;
          }
          default:
            throw new IllegalStateException("Cannot compute MV ANY_VALUE for type: " + blockValSet.getValueType());
        }
        groupByResultHolder.setValueForKey(groupKey, anyValueObject);
      }
    }
  }

  @Override
  public AnyValueObject extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public AnyValueObject extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public AnyValueObject merge(AnyValueObject intermediateMinResult1, AnyValueObject intermediateMinResult2) {
    return intermediateMinResult1.merge(intermediateMinResult2);
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public AnyValueObject extractFinalResult(AnyValueObject intermediateResult) {
    return intermediateResult;
  }

  protected void initializeColumnSchema(BlockValSet blockValSet) {
    // Initialize the schema only once
    if (_schemaInitialized.get()) {
      return;
    }

    String[] anyValueColumnNames = new String[1];
    DataSchema.ColumnDataType[] anyValueColumnDataTypes = new DataSchema.ColumnDataType[1];

    if (blockValSet.isSingleValue()) {
      switch (blockValSet.getValueType()) {
        case INT: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.INT;
          break;
        }
        case BOOLEAN: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.BOOLEAN;
          break;
        }
        case LONG:
        case TIMESTAMP: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.LONG;
          break;
        }
        case FLOAT: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.FLOAT;
          break;
        }
        case DOUBLE: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.DOUBLE;
          break;
        }
        case BIG_DECIMAL: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.BIG_DECIMAL;
          break;
        }
        case STRING:
        case JSON: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.STRING;
          break;
        }
        case BYTES: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.BYTES;
          break;
        }
        default:
          throw new IllegalStateException("Cannot set schema for ANY_VALUE aggregation for type: "
              + blockValSet.getValueType());
      }
    } else {
      switch (blockValSet.getValueType()) {
        case INT:
        case BOOLEAN: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.INT_ARRAY;
          break;
        }
        case LONG:
        case TIMESTAMP: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.LONG_ARRAY;
          break;
        }
        case FLOAT: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.FLOAT_ARRAY;
          break;
        }
        case DOUBLE: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.DOUBLE_ARRAY;
          break;
        }
        case STRING: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.STRING_ARRAY;
          break;
        }
        case BYTES: {
          anyValueColumnDataTypes[0] = DataSchema.ColumnDataType.BYTES_ARRAY;
          break;
        }
        default:
          throw new IllegalStateException("Cannot set MV schema for ANY_VALUE aggregation for type: "
              + blockValSet.getValueType());
      }
    }
    anyValueColumnNames[0] = _anyValueColumn.toString();
    _anyValueColumnSchema.set(new DataSchema(anyValueColumnNames, anyValueColumnDataTypes));
    _schemaInitialized.set(true);
  }

  private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    if (blockValSet.isSingleValue()) {
      switch (blockValSet.getValueType().getStoredType()) {
        case INT: {
          if (nullBitmap.getCardinality() < length) {
            int[] values = blockValSet.getIntValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case LONG: {
          if (nullBitmap.getCardinality() < length) {
            long[] values = blockValSet.getLongValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case FLOAT: {
          if (nullBitmap.getCardinality() < length) {
            float[] values = blockValSet.getFloatValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case DOUBLE: {
          if (nullBitmap.getCardinality() < length) {
            double[] values = blockValSet.getDoubleValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case BIG_DECIMAL: {
          if (nullBitmap.getCardinality() < length) {
            BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case STRING: {
          if (nullBitmap.getCardinality() < length) {
            String[] values = blockValSet.getStringValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case BYTES: {
          if (nullBitmap.getCardinality() < length) {
            byte[][] values = blockValSet.getBytesValuesSV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        default:
          throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
      }
    } else {
      switch (blockValSet.getValueType().getStoredType()) {
        case INT: {
          if (nullBitmap.getCardinality() < length) {
            int[][] values = blockValSet.getIntValuesMV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case LONG: {
          if (nullBitmap.getCardinality() < length) {
            long[][] values = blockValSet.getLongValuesMV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case FLOAT: {
          if (nullBitmap.getCardinality() < length) {
            float[][] values = blockValSet.getFloatValuesMV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case DOUBLE: {
          if (nullBitmap.getCardinality() < length) {
            double[][] values = blockValSet.getDoubleValuesMV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case STRING: {
          if (nullBitmap.getCardinality() < length) {
            String[][] values = blockValSet.getStringValuesMV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        case BYTES: {
          if (nullBitmap.getCardinality() < length) {
            byte[][][] values = blockValSet.getBytesValuesMV();
            for (int i = 0; i < length & i < values.length; i++) {
              if (!nullBitmap.contains(i)) {
                aggregationResultHolder.setValue(values[i]);
                break;
              }
            }
          }
          break;
        }
        default:
          throw new IllegalStateException("Cannot compute ANY_VALUE for type: " + blockValSet.getValueType());
      }
    }
  }

  private void aggregateGroupByNullHandlingEnabled(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      AnyValueObject anyValueObject = groupByResultHolder.getResult(groupKey);
      if (anyValueObject != null) {
        continue;
      }

      anyValueObject = new AnyValueObject(_anyValueColumnSchema.get());
      switch (blockValSet.getValueType().getStoredType()) {
        case INT: {
          int[] valueArray = blockValSet.getIntValuesSV();
          int value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        case LONG: {
          long[] valueArray = blockValSet.getLongValuesSV();
          long value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        case FLOAT: {
          float[] valueArray = blockValSet.getFloatValuesSV();
          float value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        case DOUBLE: {
          double[] valueArray = blockValSet.getDoubleValuesSV();
          double value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        case BIG_DECIMAL: {
          BigDecimal[] valueArray = blockValSet.getBigDecimalValuesSV();
          BigDecimal value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        case STRING: {
          String[] valueArray = blockValSet.getStringValuesSV();
          String value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        case BYTES: {
          byte[][] valueArray = blockValSet.getBytesValuesSV();
          byte[] value = valueArray[i];
          if (!nullBitmap.contains(i)) {
            anyValueObject.setValueIfNeeded(value);
            groupByResultHolder.setValueForKey(groupKey, anyValueObject);
          }
          break;
        }
        default:
          throw new IllegalStateException("Cannot compute SV group by ANY_VALUE for type: "
              + blockValSet.getValueType());
      }
    }
  }
}