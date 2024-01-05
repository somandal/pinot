package org.apache.pinot.core.query.utils.rewriter;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.utils.AnyValueObject;
import org.apache.pinot.spi.utils.CommonConstants;


public class AnyValueAggregationResultRewriter implements ResultRewriter {

  public AnyValueAggregationResultRewriter() {
  }
  @Override
  public RewriterResult rewrite(DataSchema dataSchema, List<Object[]> rows) {

    int numAnyValueFunctions = 0;
    // Count the number of ANY_VALUE aggregation functions
    for (int i = 0; i < dataSchema.size(); i++) {
      if (dataSchema.getColumnName(i).startsWith(CommonConstants.RewriterConstants.ANY_VALUE_NAME_PREFIX)) {
        numAnyValueFunctions++;
      }
    }

    if (numAnyValueFunctions == 0) {
      // no change to the result
      return new RewriterResult(dataSchema, rows);
    }

    String[] newColumnNames = new String[dataSchema.size()];
    DataSchema.ColumnDataType[] newColumnDataTypes = new DataSchema.ColumnDataType[dataSchema.size()];

    // Create a new dataSchema and new rows list based on the type of the ANY_VALUE columns
    if (!rows.isEmpty()) {
      Object[] row = rows.get(0);
      for (int i = 0; i < dataSchema.size(); i++) {
        String columnName = dataSchema.getColumnName(i);
        if (columnName.startsWith(CommonConstants.RewriterConstants.ANY_VALUE_NAME_PREFIX)) {
          // ANY_VALUE column
          AnyValueObject parent = (AnyValueObject) row[i];
          newColumnNames[i] = columnName; // no change
          newColumnDataTypes[i] = parent.getSchema().getColumnDataType(0);
        } else {
          // This is a regular column
          newColumnNames[i] = columnName;
          newColumnDataTypes[i] = dataSchema.getColumnDataType(i);
        }
      }
    }

    DataSchema newDataSchema = new DataSchema(newColumnNames, newColumnDataTypes);
    List<Object[]> newRows = new ArrayList<>();

    for (Object[] row : rows) {
      List<Object[]> newRowsBuffer = new ArrayList<>();
      Object[] newRow = new Object[newDataSchema.size()];
      for (int fieldIter = 0; fieldIter < newDataSchema.size(); fieldIter++) {
        if (newDataSchema.getColumnName(fieldIter).startsWith(CommonConstants.RewriterConstants.ANY_VALUE_NAME_PREFIX)) {
          AnyValueObject parent = (AnyValueObject) row[fieldIter];
          newRow[fieldIter] = parent.getField();
        } else {
          newRow[fieldIter] = row[fieldIter];
        }
      }
      newRowsBuffer.add(newRow);
      newRows.addAll(newRowsBuffer);
    }

    return new RewriterResult(newDataSchema, newRows);
  }
}
