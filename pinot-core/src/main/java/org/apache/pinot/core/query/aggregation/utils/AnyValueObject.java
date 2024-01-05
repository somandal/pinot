package org.apache.pinot.core.query.aggregation.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;


public class AnyValueObject implements Comparable<AnyValueObject>, Serializable {

  // if the object is created but not yet populated, this happens e.g. when a server has no data for
  // the query and returns a default value
  enum ObjectNullState {
    NULL(0),
    NON_NULL(1);

    final int _state;

    ObjectNullState(int i) {
      _state = i;
    }

    int getState() {
      return _state;
    }
  }

  // if the object contains non null values
  private boolean _isNull;

  // if the value is stored in a mutable list, this is false only when the Object is deserialized from a byte buffer
  // if the object is mutable, it means that the object is read only and the values are stored in
  // _immutableMeasuringKeys and _immutableProjectionVals, otherwise we read and write from _extremumMeasuringKeys
  // and _extremumProjectionValues
  private final boolean _mutable;

  // the schema of the column
  private final DataSchema _dataSchema;

  // the value to be stored for ANY_VALUE
  private Object[] _value = null;

  // used for ser/de
  private DataBlock _immutableValue;

  public AnyValueObject(DataSchema dataSchema) {
    _isNull = true;
    _mutable = true;
    _dataSchema = dataSchema;
  }

  public AnyValueObject(ByteBuffer byteBuffer)
      throws IOException {
    _mutable = false;
    _isNull = byteBuffer.getInt() == AnyValueObject.ObjectNullState.NULL.getState();
    byteBuffer = byteBuffer.slice();
    try {
      _immutableValue = DataBlockUtils.getDataBlock(byteBuffer);
    } catch (Exception e) {
      System.out.println("***** exception message: " + e.getMessage());
    }

    _dataSchema = _immutableValue.getDataSchema();
    System.out.println("******** get field: " + getField());
  }

  public static AnyValueObject fromBytes(byte[] bytes)
      throws IOException {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static AnyValueObject fromByteBuffer(ByteBuffer byteBuffer)
      throws IOException {
    return new AnyValueObject(byteBuffer);
  }

  @Nonnull
  public byte[] toBytes()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    if (_isNull) {
      // serialize the null object with schemas
      dataOutputStream.writeInt(AnyValueObject.ObjectNullState.NULL.getState());
      _immutableValue = DataBlockBuilder.buildFromRows(Collections.emptyList(), _dataSchema);
    } else {
      dataOutputStream.writeInt(AnyValueObject.ObjectNullState.NON_NULL.getState());
      _immutableValue =
          DataBlockBuilder.buildFromRows(Collections.singletonList(_value), _dataSchema);
    }
    dataOutputStream.write(_immutableValue.toBytes());
    return byteArrayOutputStream.toByteArray();
  }

  public void setValueIfNeeded(Object value) {
    if (_isNull) {
      _isNull = false;
      _value = new Object[1];
      _value[0] = value;
    }
  }

  public Object getField() {
    if (_mutable) {
      return _value[0];
    } else {
      switch (_dataSchema.getColumnDataType(0)) {
        case BOOLEAN:
        case INT:
          return _immutableValue.getInt(0, 0);
        case TIMESTAMP:
        case LONG:
          return _immutableValue.getLong(0, 0);
        case FLOAT:
          return _immutableValue.getFloat(0, 0);
        case DOUBLE:
          return _immutableValue.getDouble(0, 0);
        case JSON:
        case STRING:
          return _immutableValue.getString(0, 0);
        case BYTES:
          return _immutableValue.getBytes(0, 0);
        case BIG_DECIMAL:
          return _immutableValue.getBigDecimal(0, 0);
        case BOOLEAN_ARRAY:
        case INT_ARRAY:
          return _immutableValue.getIntArray(0, 0);
        case TIMESTAMP_ARRAY:
        case LONG_ARRAY:
          return _immutableValue.getLongArray(0, 0);
        case FLOAT_ARRAY:
          return _immutableValue.getFloatArray(0, 0);
        case DOUBLE_ARRAY:
          return _immutableValue.getDoubleArray(0, 0);
        case STRING_ARRAY:
        case BYTES_ARRAY:
          return _immutableValue.getStringArray(0, 0);
        default:
          throw new IllegalStateException("Unsupported data type: " + _dataSchema.getColumnDataType(0));
      }
    }
  }

  public AnyValueObject merge(AnyValueObject other) {
    if (_isNull && other._isNull) {
      return this;
    } else if (_isNull) {
      return other;
    }
    // No need to do any actual merges
    return this;
  }

  public DataSchema getSchema() {
    // the final data schema
    return _dataSchema;
  }

  @Override
  public int compareTo(AnyValueObject o) {
    return 0;
  }
}

