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
package org.apache.calcite.sql.fun;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.AggregationFunctionCalciteType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;


public class PinotOperatorTableBuiltInFunctions extends SqlStdOperatorTable {

  private static @MonotonicNonNull PinotOperatorTableBuiltInFunctions _instance;

  public static final SqlFunction COALESCE = new PinotSqlCoalesceFunction();
//  public static final SqlFunction SKEWNESS_REDUCE = new SqlFunction("SKEWNESS_REDUCE", SqlKind.OTHER_FUNCTION,
//      ReturnTypes.DOUBLE, null, OperandTypes.BINARY, SqlFunctionCategory.USER_DEFINED_FUNCTION);
//  public static final SqlFunction KURTOSIS_REDUCE = new SqlFunction("KURTOSIS_REDUCE", SqlKind.OTHER_FUNCTION,
//      ReturnTypes.DOUBLE, null, OperandTypes.BINARY, SqlFunctionCategory.USER_DEFINED_FUNCTION);
//  public static final SqlFunction COUNT_DISTINCT_REDUCE = new SqlFunction("COUNT_DISTINCT_REDUCE",
//      SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null, OperandTypes.BINARY,
//      SqlFunctionCategory.USER_DEFINED_FUNCTION);
//  public static final SqlFunction PERCENTILE_REDUCE = new SqlFunction("PERCENTILE_REDUCE",
//      SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
//      OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.NUMERIC),
//      SqlFunctionCategory.USER_DEFINED_FUNCTION);
  //public static final SqlFunction PERCENTILE = new SqlFunction("PERCENTILE", SqlKind.OTHER_FUNCTION,
  //ReturnTypes.DOUBLE,
  //    null, OperandTypes.NUMERIC_NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

  //public static final SqlAggFunction BOOL_AND = PinotBoolAndAggregateFunction.INSTANCE;
  //public static final SqlAggFunction BOOL_OR = PinotBoolOrAggregateFunction.INSTANCE;
  //public static final SqlAggFunction SKEWNESS = PinotSkewnessAggregateFunction.INSTANCE;
  //public static final SqlAggFunction KURTOSIS = PinotKurtosisAggregateFunction.INSTANCE;

  // TODO: clean up lazy init by using Suppliers.memorized(this::computeInstance) and make getter wrapped around
  // supplier instance. this should replace all lazy init static objects in the codebase
  public static synchronized PinotOperatorTableBuiltInFunctions instance() {
    if (_instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      _instance = new PinotOperatorTableBuiltInFunctions();
      _instance.initNoDuplicate();
    }
    return _instance;
  }

  /**
   * Initialize without duplicate, e.g. when 2 duplicate operator is linked with the same op
   * {@link org.apache.calcite.sql.SqlKind} it causes problem.
   *
   * <p>This is a direct copy of the {@link org.apache.calcite.sql.util.ReflectiveSqlOperatorTable} and can be hard to
   * debug, suggest changing to a non-dynamic registration. Dynamic function support should happen via catalog.
   */
  public final void initNoDuplicate() {
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null && notRegistered(op)) {
            AggregationFunctionCalciteType aggFunctionType = null;
            if (AggregationFunctionCalciteType.isAggregationFunction(op.getName())) {
              aggFunctionType = AggregationFunctionCalciteType.valueOf(op.getName());
            }
            boolean isPinotAgg = aggFunctionType != null && !aggFunctionType.isNativeCalciteAggregationFunctionType();
            System.out.println(op.getName() + " is pinot agg: " + isPinotAgg);
            if (!isPinotAgg && (aggFunctionType == null || aggFunctionType.isSupportedInMultiStage())) {
              System.out.println("Registering inbuilt function: " + op.getName() + ", is pinot agg: " + isPinotAgg);
              register(op);
            }
          }
        } else if (
            SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          if (op != null && notRegistered(op)) {
            register(op);
          }
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw Util.throwAsRuntime(Util.causeOrSelf(e));
      }
    }

    for (AggregationFunctionCalciteType aggregationFunctionType : AggregationFunctionCalciteType.values()) {
      if (!aggregationFunctionType.isNativeCalciteAggregationFunctionType()
          && aggregationFunctionType.isSupportedInMultiStage()) {
        System.out.println("Creating aggregation function for function: " + aggregationFunctionType.getName());
        PinotSqlAggFunction aggFunction = new PinotSqlAggFunction(
            aggregationFunctionType.getName().toUpperCase(Locale.ROOT),
            aggregationFunctionType.getSqlIdentifier(),
            aggregationFunctionType.getSqlKind(),
            aggregationFunctionType.getSqlReturnTypeInference(),
            aggregationFunctionType.getSqlOperandTypeInference(),
            aggregationFunctionType.getSqlOperandTypeChecker(),
            aggregationFunctionType.getSqlFunctionCategory());
        if (notRegistered(aggFunction)) {
          System.out.println("Registering aggregation function for function: "
              + aggregationFunctionType.getName());
          register(aggFunction);
        }

        if (!StringUtils.isEmpty(aggregationFunctionType.getAggregationIntermediateFunctionName())
            && !StringUtils.equals(aggregationFunctionType.getAggregationIntermediateFunctionName(),
            aggregationFunctionType.getName())) {
          System.out.println(
              "Creating intermediate aggregation function for function: "
                  + aggregationFunctionType.getAggregationIntermediateFunctionName());
          PinotSqlAggFunction intermediateAggFunction = new PinotSqlAggFunction(
              aggregationFunctionType.getAggregationIntermediateFunctionName().toUpperCase(Locale.ROOT),
              aggregationFunctionType.getSqlIdentifier(),
              aggregationFunctionType.getSqlKind(),
              aggregationFunctionType.getSqlIntermediateReturnTypeInference(),
              aggregationFunctionType.getSqlOperandTypeInference(),
              aggregationFunctionType.getSqlOperandTypeChecker(),
              aggregationFunctionType.getSqlFunctionCategory());
          if (notRegistered(intermediateAggFunction)) {
            System.out.println("Registering intermediate aggregation function for function: "
                + aggregationFunctionType.getAggregationIntermediateFunctionName());
            register(intermediateAggFunction);
          }
        }

        if (!StringUtils.isEmpty(aggregationFunctionType.getAggregationReduceFunctionName())) {
          System.out.println(
              "Creating reduce function function for function: "
                  + aggregationFunctionType.getAggregationReduceFunctionName());
          SqlFunction function = new SqlFunction(
              aggregationFunctionType.getAggregationReduceFunctionName(),
              SqlKind.OTHER_FUNCTION,
              aggregationFunctionType.getSqlReduceReturnTypeInference(),
              null,
              aggregationFunctionType.getSqlReduceOperandTypeChecker(),
              SqlFunctionCategory.USER_DEFINED_FUNCTION);
          if (notRegistered(function)) {
            System.out.println("Registering reduce aggregation function for function: "
                + aggregationFunctionType.getAggregationReduceFunctionName());
            register(function);
          }
        }
      } else {
        System.out.println("Skipping aggregation function for function: " + aggregationFunctionType.getName());
      }
    }
  }

  private boolean notRegistered(SqlFunction op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), op.getFunctionType(), op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(false));
    return operatorList.size() == 0;
  }

  private boolean notRegistered(SqlOperator op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), null, op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(false));
    return operatorList.size() == 0;
  }
}
