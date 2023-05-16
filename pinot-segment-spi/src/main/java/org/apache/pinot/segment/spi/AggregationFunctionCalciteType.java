/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.spi;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;


/**
 * NOTE: No underscore is allowed in the enum name.
 */
public enum AggregationFunctionCalciteType {
  // Aggregation functions for single-valued columns
  COUNT("count", true, null, SqlKind.COUNT, ReturnTypes.BIGINT, null,
      CalciteSystemProperty.STRICT.value() ? OperandTypes.ANY : OperandTypes.ONE_OR_MORE, SqlFunctionCategory.NUMERIC,
      true, true, null, ReturnTypes.BIGINT, "COUNT_REDUCE", ReturnTypes.INTEGER, OperandTypes.NUMERIC),
  MIN("min", true, null, SqlKind.MIN, ReturnTypes.ARG0_NULLABLE_IF_EMPTY, null,
      OperandTypes.COMPARABLE_ORDERED, SqlFunctionCategory.SYSTEM, true, true, null,
      ReturnTypes.ARG0_NULLABLE_IF_EMPTY, "MIN_REDUCE", ReturnTypes.ARG0_NULLABLE_IF_EMPTY, OperandTypes.NUMERIC),
  MAX("max", true, null, SqlKind.MAX, ReturnTypes.ARG0_NULLABLE_IF_EMPTY, null,
      OperandTypes.COMPARABLE_ORDERED, SqlFunctionCategory.SYSTEM, true, true, null,
      ReturnTypes.ARG0_NULLABLE_IF_EMPTY, "MAX_REDUCE", ReturnTypes.ARG0_NULLABLE_IF_EMPTY, OperandTypes.NUMERIC),
  SUM("sum", true, null, SqlKind.SUM, ReturnTypes.AGG_SUM, null, OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC, true, true, null, ReturnTypes.AGG_SUM, "SUM_REDUCE", ReturnTypes.AGG_SUM,
      OperandTypes.NUMERIC),
  SUM0("$sum0", true, null, SqlKind.SUM0, ReturnTypes.AGG_SUM_EMPTY_IS_ZERO, null, OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC, true, true, null, ReturnTypes.AGG_SUM_EMPTY_IS_ZERO, "SUM_REDUCE",
      ReturnTypes.AGG_SUM_EMPTY_IS_ZERO, OperandTypes.NUMERIC),
//  SUMPRECISION("sumPrecision", null, SqlKind.SUM, ReturnTypes.DECIMAL_SCALE0, null,
//      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
//          number -> number != 0),
//      SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, "sumPrecision",
//     ReturnTypes.explicit(SqlTypeName.OTHER)),
  AVG("avg", true, null, SqlKind.AVG, ReturnTypes.AVG_AGG_FUNCTION, null, OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC, true, true, null, ReturnTypes.explicit(SqlTypeName.OTHER), "AVG_REDUCE",
    ReturnTypes.AVG_AGG_FUNCTION, OperandTypes.BINARY),
  MODE("mode", true, null, SqlKind.MODE, ReturnTypes.ARG0_NULLABLE_IF_EMPTY, null,
      OperandTypes.or(OperandTypes.NUMERIC,
          OperandTypes.and(
              OperandTypes.sequence("'MODE(<NUMERIC>, <STRING_LITERAL>)'",
                  OperandTypes.NUMERIC,
                  OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING))),
     SqlFunctionCategory.NUMERIC, false, true, "mode",
     ReturnTypes.explicit(SqlTypeName.OTHER), "MODE_REDUCE", ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
     OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.STRING)),
  SKEWNESS("skewness", true, null, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE,
      null, OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, "fourthmoment",
              ReturnTypes.explicit(SqlTypeName.OTHER), "SKEWNESS_REDUCE", ReturnTypes.DOUBLE, OperandTypes.BINARY),
  KURTOSIS("kurtosis", true, null, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE,
      null, OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, "fourthmoment",
      ReturnTypes.explicit(SqlTypeName.OTHER), "KURTOSIS_REDUCE", ReturnTypes.DOUBLE, OperandTypes.BINARY),
//  FOURTHMOMENT("fourthmoment", null, SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(SqlTypeName.OTHER),
//      null, OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, null),
  DISTINCTCOUNT("distinctCount", true, null, SqlKind.COUNT, ReturnTypes.BIGINT, null,
      OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, null,
    ReturnTypes.explicit(SqlTypeName.OTHER), "COUNT_DISTINCT_REDUCE", ReturnTypes.INTEGER, OperandTypes.BINARY),
  PERCENTILE("percentile", true, null, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
      OperandTypes.and(
          OperandTypes.sequence("'PERCENTILE(column, probability)'",
              OperandTypes.NUMERIC,
              OperandTypes.LITERAL),
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
      ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, null,
    ReturnTypes.explicit(SqlTypeName.OTHER), "PERCENTILE_REDUCE", ReturnTypes.DOUBLE,
    OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.NUMERIC)),
  PERCENTILETDIGEST("percentileTDigest", true, null, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
      OperandTypes.or(
          OperandTypes.and(
              OperandTypes.sequence("'PERCENTILETDIGEST(column, probability)'",
                  OperandTypes.NUMERIC, OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
          ),
          OperandTypes.and(
              OperandTypes.sequence("'PERCENTILETDIGEST(column, probability, compressionFactor)'",
                  OperandTypes.NUMERIC, OperandTypes.LITERAL, OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
          )),
      SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, null,
      ReturnTypes.explicit(SqlTypeName.OTHER), "PERCENTILE_TDIGEST_REDUCE", ReturnTypes.DOUBLE,
      OperandTypes.or(
          OperandTypes.and(
              OperandTypes.sequence("'PERCENTILE_TDIGEST_REDUCE(column, probability)'",
                  OperandTypes.BINARY, OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.NUMERIC)
          ),
          OperandTypes.and(
              OperandTypes.sequence("'PERCENTILE_TDIGEST_REDUCE(column, probability, compressionFactor)'",
                  OperandTypes.BINARY, OperandTypes.LITERAL, OperandTypes.LITERAL),
              OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
          ))),
 // DISTINCTCOUNT("DISTINCTCOUNT", null, SqlKind.OTHER_FUNCTION, ReturnTypes.explicit(SqlTypeName.OTHER),
  //    null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION,),
  /*FIRSTWITHTIME("firstWithTime"),
  LASTWITHTIME("lastWithTime"),
  MINMAXRANGE("minMaxRange"),
  DISTINCTCOUNT("distinctCount"),
  DISTINCTCOUNTBITMAP("distinctCountBitmap"),
  SEGMENTPARTITIONEDDISTINCTCOUNT("segmentPartitionedDistinctCount"),
  DISTINCTCOUNTHLL("distinctCountHLL"),
  DISTINCTCOUNTRAWHLL("distinctCountRawHLL"),
  DISTINCTCOUNTSMARTHLL("distinctCountSmartHLL"),
  FASTHLL("fastHLL"),
  DISTINCTCOUNTTHETASKETCH("distinctCountThetaSketch"),
  DISTINCTCOUNTRAWTHETASKETCH("distinctCountRawThetaSketch"),
  DISTINCTSUM("distinctSum"),
  DISTINCTAVG("distinctAvg"),
  PERCENTILE("percentile"),
  PERCENTILEEST("percentileEst"),
  PERCENTILERAWEST("percentileRawEst"),
  PERCENTILETDIGEST("percentileTDigest"),
  PERCENTILERAWTDIGEST("percentileRawTDigest"),
  PERCENTILESMARTTDIGEST("percentileSmartTDigest"),
  IDSET("idSet"),
  HISTOGRAM("histogram"),
  COVARPOP("covarPop"),
  COVARSAMP("covarSamp"),
  VARPOP("varPop"),
  VARSAMP("varSamp"),
  STDDEVPOP("stdDevPop"),
  STDDEVSAMP("stdDevSamp"),
  SKEWNESS("skewness"),
  KURTOSIS("kurtosis"),
  FOURTHMOMENT("fourthmoment"),

  // Geo aggregation functions
  STUNION("STUnion"),

  // Aggregation functions for multi-valued columns
  COUNTMV("countMV"),
  MINMV("minMV"),
  MAXMV("maxMV"),
  SUMMV("sumMV"),
  AVGMV("avgMV"),
  MINMAXRANGEMV("minMaxRangeMV"),
  DISTINCTCOUNTMV("distinctCountMV"),
  DISTINCTCOUNTBITMAPMV("distinctCountBitmapMV"),
  DISTINCTCOUNTHLLMV("distinctCountHLLMV"),
  DISTINCTCOUNTRAWHLLMV("distinctCountRawHLLMV"),
  DISTINCTSUMMV("distinctSumMV"),
  DISTINCTAVGMV("distinctAvgMV"),
  PERCENTILEMV("percentileMV"),
  PERCENTILEESTMV("percentileEstMV"),
  PERCENTILERAWESTMV("percentileRawEstMV"),
  PERCENTILETDIGESTMV("percentileTDigestMV"),
  PERCENTILERAWTDIGESTMV("percentileRawTDigestMV"),
  DISTINCT("distinct"),*/

  // boolean aggregate functions
  BOOLAND("bool_And", true, null, SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN,
      null, OperandTypes.BOOLEAN, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, null, ReturnTypes.BOOLEAN,
      "BOOL_AND_REDUCER", ReturnTypes.BOOLEAN, OperandTypes.BOOLEAN),
  BOOLOR("bool_Or", true, null, SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN,
      null, OperandTypes.BOOLEAN, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, true, null, ReturnTypes.BOOLEAN,
      "BOOL_OR_REDUCER", ReturnTypes.BOOLEAN, OperandTypes.BOOLEAN);

  private static final Set<String> NAMES = Arrays.stream(values()).flatMap(func -> Stream.of(func.name(),
      func.getName(), func.getName().toLowerCase())).collect(Collectors.toSet());

  private static final Set<SqlKind> KINDS = Arrays.stream(values()).flatMap(func -> Stream.of(func.getSqlKind()))
      .collect(Collectors.toSet());

  private final String _name;
  private final boolean _isSupportedInMultiStage;
  private final SqlIdentifier _sqlIdentifier;
  private final SqlKind _sqlKind;
  private final SqlReturnTypeInference _sqlReturnTypeInference;
  private final SqlOperandTypeInference _sqlOperandTypeInference;
  private final SqlOperandTypeChecker _sqlOperandTypeChecker;
  private final SqlFunctionCategory _sqlFunctionCategory;
  private final boolean _isNativeCalciteAggregationFunctionType;
  private final boolean _isSingleValueFunction;
  private final String _aggregationIntermediateFunctionName;
  private final SqlReturnTypeInference _sqlIntermediateReturnTypeInference;
  private final String _aggregationReduceFunctionName;
  private final SqlReturnTypeInference _sqlReduceReturnTypeInference;
  private final SqlOperandTypeChecker _sqlReduceOperandTypeChecker;

  AggregationFunctionCalciteType(String name, boolean isSupportedInMultiStage, SqlIdentifier sqlIdentifier,
      SqlKind sqlKind, SqlReturnTypeInference sqlReturnTypeInference, SqlOperandTypeInference sqlOperandTypeInference,
      SqlOperandTypeChecker sqlOperandTypeChecker, SqlFunctionCategory sqlFunctionCategory,
      boolean isNativeCalciteAggregationFunctionType, boolean isSingleValueFunction,
      String aggregationIntermediateFunctionName, SqlReturnTypeInference sqlIntermediateReturnTypeInference,
      String aggregationReduceFunctionName, SqlReturnTypeInference sqlReduceReturnTypeInference,
      SqlOperandTypeChecker sqlReduceOperandTypeChecker) {
    _name = name;
    _isSupportedInMultiStage = isSupportedInMultiStage;
    _sqlIdentifier = sqlIdentifier;
    _sqlKind = sqlKind;
    _sqlReturnTypeInference = sqlReturnTypeInference;
    _sqlOperandTypeInference = sqlOperandTypeInference;
    _sqlOperandTypeChecker = sqlOperandTypeChecker;
    _sqlFunctionCategory = sqlFunctionCategory;
    _isNativeCalciteAggregationFunctionType = isNativeCalciteAggregationFunctionType;
    _isSingleValueFunction = isSingleValueFunction;
    _aggregationIntermediateFunctionName = aggregationIntermediateFunctionName == null
        ? name : aggregationIntermediateFunctionName;
    _sqlIntermediateReturnTypeInference = sqlIntermediateReturnTypeInference;
    _aggregationReduceFunctionName = aggregationReduceFunctionName;
    _sqlReduceReturnTypeInference = sqlReduceReturnTypeInference;
    _sqlReduceOperandTypeChecker = sqlReduceOperandTypeChecker;
  }

  public String getName() {
    return _name;
  }

  public boolean isSupportedInMultiStage() {
    return _isSupportedInMultiStage;
  }

  public SqlIdentifier getSqlIdentifier() {
    return _sqlIdentifier;
  }

  public SqlKind getSqlKind() {
    return _sqlKind;
  }

  public SqlReturnTypeInference getSqlReturnTypeInference() {
    return _sqlReturnTypeInference;
  }

  public SqlOperandTypeInference getSqlOperandTypeInference() {
    return _sqlOperandTypeInference;
  }

  public SqlOperandTypeChecker getSqlOperandTypeChecker() {
    return _sqlOperandTypeChecker;
  }

  public SqlFunctionCategory getSqlFunctionCategory() {
    return _sqlFunctionCategory;
  }

  public boolean isNativeCalciteAggregationFunctionType() {
    return _isNativeCalciteAggregationFunctionType;
  }

  public boolean isSingleValueFunction() {
    return _isSingleValueFunction;
  }

  public String getAggregationIntermediateFunctionName() {
    return _aggregationIntermediateFunctionName;
  }

  public SqlReturnTypeInference getSqlIntermediateReturnTypeInference() {
    return _sqlIntermediateReturnTypeInference;
  }

  public String getAggregationReduceFunctionName() {
    return _aggregationReduceFunctionName;
  }

  public SqlReturnTypeInference getSqlReduceReturnTypeInference() {
    return _sqlReduceReturnTypeInference;
  }

  public SqlOperandTypeChecker getSqlReduceOperandTypeChecker() {
    return _sqlReduceOperandTypeChecker;
  }

  public static boolean isAggregationFunction(String functionName) {
    if (NAMES.contains(functionName)) {
      return true;
    }
    if (functionName.regionMatches(true, 0, "percentile", 0, 10)) {
      try {
        getAggregationFunctionType(functionName);
        return true;
      } catch (Exception ignore) {
        return false;
      }
    }
    String upperCaseFunctionName = StringUtils.remove(functionName, '_').toUpperCase();
    return NAMES.contains(upperCaseFunctionName);
  }

  public static boolean isAggregationKindSupported(SqlKind kind) {
    System.out.println("** KINDS: " + KINDS);
    return KINDS.contains(kind);
  }

  /**
   * Returns the corresponding aggregation function type for the given function name.
   * <p>NOTE: Underscores in the function name are ignored.
   */
  public static AggregationFunctionCalciteType getAggregationFunctionType(String functionName) {
    /*if (functionName.regionMatches(true, 0, "percentile", 0, 10)) {
      String remainingFunctionName = StringUtils.remove(functionName, '_').substring(10).toUpperCase();
      if (remainingFunctionName.isEmpty() || remainingFunctionName.matches("\\d+")) {
        return PERCENTILE;
      } else if (remainingFunctionName.equals("EST") || remainingFunctionName.matches("EST\\d+")) {
        return PERCENTILEEST;
      } else if (remainingFunctionName.equals("RAWEST") || remainingFunctionName.matches("RAWEST\\d+")) {
        return PERCENTILERAWEST;
      } else if (remainingFunctionName.equals("TDIGEST") || remainingFunctionName.matches("TDIGEST\\d+")) {
        return PERCENTILETDIGEST;
      } else if (remainingFunctionName.equals("RAWTDIGEST") || remainingFunctionName.matches("RAWTDIGEST\\d+")) {
        return PERCENTILERAWTDIGEST;
      } else if (remainingFunctionName.equals("MV") || remainingFunctionName.matches("\\d+MV")) {
        return PERCENTILEMV;
      } else if (remainingFunctionName.equals("ESTMV") || remainingFunctionName.matches("EST\\d+MV")) {
        return PERCENTILEESTMV;
      } else if (remainingFunctionName.equals("RAWESTMV") || remainingFunctionName.matches("RAWEST\\d+MV")) {
        return PERCENTILERAWESTMV;
      } else if (remainingFunctionName.equals("TDIGESTMV") || remainingFunctionName.matches("TDIGEST\\d+MV")) {
        return PERCENTILETDIGESTMV;
      } else if (remainingFunctionName.equals("RAWTDIGESTMV") || remainingFunctionName.matches("RAWTDIGEST\\d+MV")) {
        return PERCENTILERAWTDIGESTMV;
      } else {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }*/
    //} else {
      try {
        return AggregationFunctionCalciteType.valueOf(StringUtils.remove(functionName, '_').toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    //}
  }
}
