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

package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.PinotFourthMomentAggregateFunction;
import org.apache.calcite.sql.fun.PinotKurtosisAggregateFunction;
import org.apache.calcite.sql.fun.PinotSkewnessAggregateFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.segment.spi.AggregationFunctionCalciteType;


/**
 * This rule rewrites aggregate functions when necessary for Pinot's
 * multistage engine. For example, SKEWNESS must be rewritten into two
 * parts: a multi-stage FOURTH_MOMENT calculation and then a scalar function
 * that reduces the moment into the skewness at the end. This is to ensure
 * that the aggregation computation can merge partial results from different
 * intermediate nodes before reducing it into the final result.
 *
 * <p>This implementation follows closely with Calcite's
 * {@link AggregateReduceFunctionsRule}.
 */
public class PinotReduceAggregateFunctionsRule extends RelOptRule {

  public static final PinotReduceAggregateFunctionsRule INSTANCE =
      new PinotReduceAggregateFunctionsRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public static final RelHint REDUCER_ADDED_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_REDUCER_ADDED).build();

  private static final Set<String> FUNCTIONS = ImmutableSet.of(
      PinotSkewnessAggregateFunction.SKEWNESS,
      PinotKurtosisAggregateFunction.KURTOSIS,
      "PERCENTILE",
      "PERCENTILETDIGEST",
      "SUM",
      "$SUM0",
      "COUNT",
      "MIN",
      "MAX",
      "DISTINCTCOUNT"
  );

  protected PinotReduceAggregateFunctionsRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }

    if (call.rel(0) instanceof Aggregate) {
      Aggregate agg = call.rel(0);
      for (AggregateCall aggCall : agg.getAggCallList()) {
        System.out.println("Agg call name to reduce: " + aggCall.getAggregation().getName());
        if (shouldReduce(aggCall) && !PinotHintStrategyTable.containsHint(
            agg.getHints(), PinotHintStrategyTable.INTERNAL_AGG_REDUCER_ADDED)) {
          System.out.println("Yes Agg call name to reduce: " + aggCall.getAggregation().getName());
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    System.out.println("*** Calling PinotReduceAggregateFunctionsRule");
    Aggregate oldAggRel = call.rel(0);
    reduceAggs(call, oldAggRel);
  }

  private void reduceAggs(RelOptRuleCall ruleCall, Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int groupCount = oldAggRel.getGroupCount();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    final List<RexNode> projList = new ArrayList<>();
    final List<String> projListNames = new ArrayList<>();

    // pass through group key
    for (int i = 0; i < groupCount; i++) {
      projList.add(rexBuilder.makeInputRef(oldAggRel, i));
      projListNames.add(oldAggRel.getRowType().getFieldList().get(i).getName());
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra project
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new aggregate function calls and rest of project list together
    int count = projList.size();
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
      projListNames.add("$" + count++);
    }

    final int extraArgCount = inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.nCopies(extraArgCount, null)));
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    newCalcRel(relBuilder, projListNames, projList);
    ruleCall.transformTo(relBuilder.build());
  }

  private RexNode reduceAgg(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs) {
    if (shouldReduce(oldCall)) {
      switch (oldCall.getAggregation().getName()) {
        case PinotSkewnessAggregateFunction.SKEWNESS:
          return reduceFourthMoment(oldAggRel, oldCall, newCalls, aggCallMapping, false);
        case PinotKurtosisAggregateFunction.KURTOSIS:
          return reduceFourthMoment(oldAggRel, oldCall, newCalls, aggCallMapping, true);
        case "COUNT":
        case "DISTINCTCOUNT":
          if (oldCall.isDistinct() || oldCall.getAggregation().getName().equals("DISTINCTCOUNT")) {
            System.out.println("****** is distinct count");
            return reduceCountDistinct(oldAggRel, oldCall, newCalls, aggCallMapping);
          } else {
            System.out.println("****** is not distinct count");
            return reduceAggregation(oldAggRel, oldCall, newCalls, aggCallMapping);
          }
        case "PERCENTILE":
        case "PERCENTILETDIGEST":
          return reducePercentile(oldAggRel, oldCall, newCalls, aggCallMapping);
        case "MAX":
        case "MIN":
        case "SUM":
        case "$SUM0":
          return reduceAggregation(oldAggRel, oldCall, newCalls, aggCallMapping);
        default:
          throw new IllegalStateException("Unexpected aggregation name " + oldCall.getAggregation().getName());
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();
      return rexBuilder.addAggCall(oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldAggRel.getInput()::fieldIsNullable);
    }
  }

  private RexNode reduceCountDistinct(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    String functionName = oldCall.getAggregation().getName();
    if (oldCall.getAggregation().getName().equalsIgnoreCase("COUNT") && oldCall.isDistinct()) {
      functionName = "distinctCount";
    }
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    AggregationFunctionCalciteType type = AggregationFunctionCalciteType.getAggregationFunctionType(
        functionName);
    SqlAggFunction percentileAggFunction = new PinotSqlAggFunction(
        type.getAggregationIntermediateFunctionName().toUpperCase(Locale.ROOT),
        type.getSqlIdentifier(), type.getSqlKind(), type.getSqlIntermediateReturnTypeInference(),
        type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(), type.getSqlFunctionCategory());
    final AggregateCall countDistinctCall =
        AggregateCall.create(percentileAggFunction /*PinotDistinctCountAggregateFunction.INSTANCE*/,
            oldCall.getAggregation().getName().equalsIgnoreCase("DISTINCTCOUNT") || oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    RexNode countDistinctRef = rexBuilder.addAggCall(countDistinctCall, nGroups, newCalls,
        aggCallMapping, oldAggRel.getInput()::fieldIsNullable);

    SqlFunction function = new SqlFunction(type.getAggregationReduceFunctionName(), SqlKind.OTHER_FUNCTION,
        type.getSqlReduceReturnTypeInference(), null, type.getSqlReduceOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

    final RexNode reduceRef = rexBuilder.makeCall(
        //PinotOperatorTable.COUNT_DISTINCT_REDUCE,
        //PinotOperatorTableBuiltInFunctions.COUNT_DISTINCT_REDUCE,
        function,
        countDistinctRef);
    return rexBuilder.makeCast(oldCall.getType(), reduceRef);
  }

  private RexNode reducePercentile(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    AggregationFunctionCalciteType type = AggregationFunctionCalciteType.getAggregationFunctionType(
        oldCall.getAggregation().getName());
    SqlAggFunction aggFunction =
        new PinotSqlAggFunction(type.getAggregationIntermediateFunctionName().toUpperCase(Locale.ROOT),
            type.getSqlIdentifier(), type.getSqlKind(), type.getSqlIntermediateReturnTypeInference(),
            type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(), type.getSqlFunctionCategory());
    final AggregateCall percentileCall =
        AggregateCall.create(aggFunction,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

   /* SqlAggFunction percentileAggFunction2 = new PinotSqlAggFunction(type.getAggregationIntermediateFunctionName(),
        type.getSqlIdentifier(), type.getSqlKind(), type.getSqlReturnTypeInference(),
        type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(), type.getSqlFunctionCategory());
    final RelDataTypeFactory typeFactory =
        oldAggRel.getInput().getCluster().getTypeFactory();
    final List<RelDataType> types =
        SqlTypeUtil.projectTypes(oldAggRel.getInput().getRowType(), oldCall.getArgList());
    final Aggregate.AggCallBinding callBinding =
        new Aggregate.AggCallBinding(typeFactory, percentileAggFunction2, types,
            oldAggRel.getGroupCount(), oldAggRel.getGroupCount() >= 0);
    RelDataType relDataType = percentileAggFunction2.inferReturnType(callBinding);*/

    System.out.println("*** old arglist: " + oldCall.getArgList());
    Set<Integer> argListSet = new HashSet<>(oldCall.getArgList());
    // TODO: This only works for a single argument (i.e. for general expressions, literals args are added later)
    RexNode ref = rexBuilder.addAggCall(percentileCall, nGroups, newCalls, aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
    List<RexNode> functionArgs = new ArrayList<>();
    functionArgs.add(ref);
    if (PinotRuleUtils.isProject(oldAggRel.getInput())) {
      LogicalProject project = (LogicalProject) ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
      int fieldCountProject = project.getRowType().getFieldCount();
      for (int i = 0; i < fieldCountProject; i++) {
        if (project.getProjects().get(i).getKind() == SqlKind.LITERAL) {
          if (argListSet.contains(i)) {
            functionArgs.add(project.getProjects().get(i));
          }
        }
      }
    }

    SqlFunction function = new SqlFunction(type.getAggregationReduceFunctionName(), SqlKind.OTHER_FUNCTION,
        type.getSqlReturnTypeInference(), null, type.getSqlReduceOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

    final RexNode reduceRef = rexBuilder.makeCall(
        //PinotOperatorTable.COUNT_DISTINCT_REDUCE,
        function,
        functionArgs);
    return rexBuilder.makeCast(oldCall.getType(), reduceRef);
  }

  private RexNode reduceAggregation(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    AggregationFunctionCalciteType type = AggregationFunctionCalciteType.getAggregationFunctionType(
        oldCall.getAggregation().getName());
    SqlAggFunction aggFunction =
        new PinotSqlAggFunction(type.getAggregationIntermediateFunctionName().toUpperCase(Locale.ROOT),
            type.getSqlIdentifier(), type.getSqlKind(), type.getSqlIntermediateReturnTypeInference(),
            type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(), type.getSqlFunctionCategory());
    final AggregateCall percentileCall =
        AggregateCall.create(aggFunction,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

   /* SqlAggFunction percentileAggFunction2 = new PinotSqlAggFunction(type.getAggregationIntermediateFunctionName(),
        type.getSqlIdentifier(), type.getSqlKind(), type.getSqlReturnTypeInference(),
        type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(), type.getSqlFunctionCategory());
    final RelDataTypeFactory typeFactory =
        oldAggRel.getInput().getCluster().getTypeFactory();
    final List<RelDataType> types =
        SqlTypeUtil.projectTypes(oldAggRel.getInput().getRowType(), oldCall.getArgList());
    final Aggregate.AggCallBinding callBinding =
        new Aggregate.AggCallBinding(typeFactory, percentileAggFunction2, types,
            oldAggRel.getGroupCount(), oldAggRel.getGroupCount() >= 0);
    RelDataType relDataType = percentileAggFunction2.inferReturnType(callBinding);*/

    System.out.println("*** old arglist: " + oldCall.getArgList());
    Set<Integer> argListSet = new HashSet<>(oldCall.getArgList());
    // TODO: This only works for a single argument (i.e. for general expressions, literals args are added later)
    RexNode ref = rexBuilder.addAggCall(percentileCall, nGroups, newCalls, aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
    List<RexNode> functionArgs = new ArrayList<>();
    functionArgs.add(ref);
    if (PinotRuleUtils.isProject(oldAggRel.getInput())) {
      LogicalProject project = (LogicalProject) ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
      int fieldCountProject = project.getRowType().getFieldCount();
      for (int i = 0; i < fieldCountProject; i++) {
        if (project.getProjects().get(i).getKind() == SqlKind.LITERAL) {
          if (argListSet.contains(i)) {
            functionArgs.add(project.getProjects().get(i));
          }
        }
      }
    }

    SqlFunction function = new SqlFunction(type.getAggregationReduceFunctionName(), SqlKind.OTHER_FUNCTION,
        type.getSqlReturnTypeInference(), null, type.getSqlReduceOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

    final RexNode reduceRef = rexBuilder.makeCall(
        //PinotOperatorTable.COUNT_DISTINCT_REDUCE,
        function,
        functionArgs);
    return rexBuilder.makeCast(oldCall.getType(), reduceRef);
  }

  private RexNode reduceFourthMoment(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping, boolean isKurtosis) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    AggregationFunctionCalciteType type = AggregationFunctionCalciteType.getAggregationFunctionType(
        oldCall.getAggregation().getName());
    final AggregateCall fourthMomentCall =
        AggregateCall.create(PinotFourthMomentAggregateFunction.INSTANCE,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    RexNode fmRef = rexBuilder.addAggCall(fourthMomentCall, nGroups, newCalls,
        aggCallMapping, oldAggRel.getInput()::fieldIsNullable);

    SqlFunction function = new SqlFunction(type.getAggregationReduceFunctionName(), SqlKind.OTHER_FUNCTION,
        type.getSqlReturnTypeInference(), null, type.getSqlReduceOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

    final RexNode skewRef = rexBuilder.makeCall(
        //isKurtosis ? PinotOperatorTable.KURTOSIS_REDUCE : PinotOperatorTable.SKEWNESS_REDUCE,
        //isKurtosis ? PinotOperatorTableBuiltInFunctions.KURTOSIS_REDUCE
        //    : PinotOperatorTableBuiltInFunctions.SKEWNESS_REDUCE,
        function, fmRef);
    return rexBuilder.makeCast(oldCall.getType(), skewRef);
  }

  private boolean shouldReduce(AggregateCall call) {
    String name = call.getAggregation().getName();
    // special case COUNT because it should only be reduced when it's a
    // COUNT DISTINCT
    return name.equals("COUNT") ? call.isDistinct() || !call.isDistinct() : FUNCTIONS.contains(name);
  }

  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate,
      List<AggregateCall> newCalls) {
    ImmutableList<RelHint> orgHints = oldAggregate.getHints();
    ImmutableList<RelHint> newAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(REDUCER_ADDED_HINT).build();
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()),
        newCalls);
    System.out.println("**** Adding the new hints: " + newAggHints);
    relBuilder.hints(newAggHints);
  }

  protected void newCalcRel(RelBuilder relBuilder,
      RelDataType rowType,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }
  protected void newCalcRel(RelBuilder relBuilder,
      List<String> rowTypeNames,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowTypeNames);
  }
}
