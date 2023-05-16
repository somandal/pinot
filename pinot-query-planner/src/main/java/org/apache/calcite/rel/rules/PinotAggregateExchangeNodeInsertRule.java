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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.segment.spi.AggregationFunctionCalciteType;


/**
 * Special rule for Pinot, this rule is fixed to generate a 2-stage aggregation split between the
 * (1) non-data-locale Pinot server agg stage, and (2) the data-locale Pinot intermediate agg stage.
 *
 * Pinot uses special intermediate data representation for partially aggregated results, thus we can't use
 * {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule} to reduce complex aggregation.
 *
 * This rule is here to introduces Pinot-special aggregation splits. In-general, all aggregations are split into
 * intermediate-stage AGG; and server-stage AGG with the same naming. E.g.
 *
 * COUNT(*) transforms into: COUNT(*)_SERVER --> COUNT(*)_INTERMEDIATE, where
 *   COUNT(*)_SERVER produces TUPLE[ SUM(1), GROUP_BY_KEY ]
 *   COUNT(*)_INTERMEDIATE produces TUPLE[ SUM(COUNT(*)_SERVER), GROUP_BY_KEY ]
 *
 * However, the suffix _SERVER/_INTERMEDIATE is merely a SQL hint to the Aggregate operator and will be translated
 * into correct, actual operator chain during Physical plan.
 */
public class PinotAggregateExchangeNodeInsertRule extends RelOptRule {
  public static final PinotAggregateExchangeNodeInsertRule INSTANCE =
      new PinotAggregateExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);
  private static final Set<SqlKind> SUPPORTED_AGG_KIND = ImmutableSet.of(
      SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT, SqlKind.OTHER_FUNCTION);
  private static final RelHint FINAL_STAGE_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE).build();
  private static final RelHint INTERMEDIATE_STAGE_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE).build();

  public PinotAggregateExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Aggregate) {
      Aggregate agg = call.rel(0);
      ImmutableList<RelHint> hints = agg.getHints();
      return !PinotHintStrategyTable.containsHint(hints, PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE)
          && !PinotHintStrategyTable.containsHint(hints, PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE)
          && !PinotHintStrategyTable.containsHintOption(hints, PinotHintOptions.AGGREGATE_HINT_OPTIONS,
          PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS);
    }
    return false;
  }

  /**
   * Split the AGG into 2 plan fragments, both with the same AGG type,
   * Pinot internal plan fragment optimization can use the info of the input data type to infer whether it should
   * generate
   * the "intermediate-stage AGG operator" or a "leaf-stage AGG operator"
   * @see org.apache.pinot.core.query.aggregation.function.AggregationFunction
   *
   * @param call the {@link RelOptRuleCall} on match.
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    ImmutableList<RelHint> oldHints = oldAggRel.getHints();

    // If "skipLeafStageGroupByAggregation" SQLHint is passed, the leaf stage aggregation is skipped. This only
    // applies for Group By Aggregations.
    if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHint(oldHints,
        PinotHintStrategyTable.SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION)) {
      // This is not the default path. Use this group by optimization to skip leaf stage aggregation when aggregating
      // at leaf level could be wasted effort. eg: when cardinality of group by column is very high.
      createPlanWithoutLeafAggregation(call);
      return;
    }

    // 1. attach leaf agg RelHint to original agg.
    ImmutableList<RelHint> newLeafAggHints =
        new ImmutableList.Builder<RelHint>().addAll(oldHints).add(INTERMEDIATE_STAGE_HINT).build();
    Aggregate newLeafAgg =
        new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
            oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());

    Set<Integer> potentialAdditionalLiteralProjectColumns = new HashSet<>();
    List<AggregateCall> aggregateCallList = oldAggRel.getAggCallList();
    for (AggregateCall aggregateCall : aggregateCallList) {
      List<Integer> argList = aggregateCall.getArgList();
      if (argList.size() == 1) {
        continue;
      }
      for (int j = 1; j < argList.size(); j++) {
        potentialAdditionalLiteralProjectColumns.add(argList.get(j));
      }
    }

    RelNode inputToExchange = newLeafAgg;
    RelOptCluster cluster = oldAggRel.getCluster();
    final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
    final List<RexNode> expsForProjectAboveWindow = new ArrayList<>();
    final List<String> expsFieldNamesAboveWindow = new ArrayList<>();
    final List<RelDataTypeField> rowTypeAggFieldList = newLeafAgg.getRowType().getFieldList();
    int count = 0;
    for (RelDataTypeField field : rowTypeAggFieldList) {
      System.out.println("* ** field type: " + field.getType() + ", field name: " + field.getName());
      expsForProjectAboveWindow.add(new RexInputRef(count++, field.getType()));
      builder.add(field);
      expsFieldNamesAboveWindow.add(field.getName());
    }
    Map<Integer, Integer> literalOldProjectToNewProjectIndexMap = new HashMap<>();
    boolean needToCreateProject = false;
    if (PinotRuleUtils.isProject(newLeafAgg.getInput())) {
      // Need to add a project on top of this agg
      LogicalProject existingProject = (LogicalProject) ((HepRelVertex) newLeafAgg.getInput()).getCurrentRel();
      List<RexNode> existingProjectNodes = existingProject.getProjects();
      for (int i = 0; i < existingProjectNodes.size(); i++) {
        RexNode rexNode = existingProjectNodes.get(i);
        if (rexNode.getKind() == SqlKind.LITERAL && potentialAdditionalLiteralProjectColumns.contains(i)) {
          needToCreateProject = true;
          literalOldProjectToNewProjectIndexMap.put(i, count);
          expsForProjectAboveWindow.add(rexNode);
          expsFieldNamesAboveWindow.add("$lf" + count++);
        }
      }
      if (needToCreateProject) {
        inputToExchange = LogicalProject
            .create(newLeafAgg, existingProject.getHints(), expsForProjectAboveWindow, expsFieldNamesAboveWindow);
      }
    }

    // 2. attach exchange.
    List<Integer> groupSetIndices = ImmutableIntList.range(0, oldAggRel.getGroupCount());
    LogicalExchange exchange;
    if (groupSetIndices.size() == 0) {
      exchange = LogicalExchange.create(inputToExchange/*newLeafAgg*/, RelDistributions.hash(Collections.emptyList()));
    } else {
      exchange = LogicalExchange.create(inputToExchange /*newLeafAgg*/, RelDistributions.hash(groupSetIndices));
    }

    // 3. attach intermediate agg stage.
    RelNode newAggNode = makeNewIntermediateAgg(call, oldAggRel, exchange, true, null, null,
        literalOldProjectToNewProjectIndexMap);
    newAggNode.getRowType();
    call.transformTo(newAggNode);
  }

  private RelNode makeNewIntermediateAgg(RelOptRuleCall ruleCall, Aggregate oldAggRel, LogicalExchange exchange,
      boolean isLeafStageAggregationPresent, @Nullable List<Integer> argList, @Nullable List<Integer> groupByList,
      @Nullable Map<Integer, Integer> literalIndexMap) {

    // add the exchange as the input node to the relation builder.
    RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(exchange);

    // make input ref to the exchange after the leaf aggregate.
    RexBuilder rexBuilder = exchange.getCluster().getRexBuilder();
    final int nGroups = oldAggRel.getGroupCount();
    for (int i = 0; i < nGroups; i++) {
      rexBuilder.makeInputRef(oldAggRel, i);
    }

    // create new aggregate function calls from exchange input.
    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    List<AggregateCall> newCalls = new ArrayList<>();
    Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    for (int oldCallIndex = 0; oldCallIndex < oldCalls.size(); oldCallIndex++) {
      AggregateCall oldCall = oldCalls.get(oldCallIndex);
      convertAggCall(rexBuilder, oldAggRel, oldCallIndex, oldCall, newCalls, aggCallMapping,
          isLeafStageAggregationPresent, argList, literalIndexMap);
    }

    // create new aggregate relation.
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();
    ImmutableList<RelHint> newIntermediateAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(FINAL_STAGE_HINT).build();
    ImmutableBitSet groupSet = groupByList == null ? ImmutableBitSet.range(nGroups) : ImmutableBitSet.of(groupByList);
    relBuilder.aggregate(
        relBuilder.groupKey(groupSet, ImmutableList.of(groupSet)),
        newCalls);
    relBuilder.hints(newIntermediateAggHints);
    return relBuilder.build();
  }

  /**
   * convert aggregate call based on the intermediate stage input.
   *
   * <p>Note that the intermediate stage input only supports splittable aggregators such as SUM/MIN/MAX.
   * All non-splittable aggregator must be converted into splittable aggregator first.
   *
   * <p>For COUNT operations, the intermediate stage will be converted to SUM.
   */
  private static void convertAggCall(RexBuilder rexBuilder, Aggregate oldAggRel, int oldCallIndex,
      AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping,
      boolean isLeafStageAggregationPresent, @Nullable List<Integer> argList,
      @Nullable Map<Integer, Integer> literalIndexMap) {
    final int nGroups = oldAggRel.getGroupCount();
    final SqlAggFunction oldAggregation = oldCall.getAggregation();
    final SqlKind aggKind = oldAggregation.getKind();
    // Check only the supported AGG functions are provided.
    Preconditions.checkState(AggregationFunctionCalciteType.isAggregationFunction(oldAggregation.getName())
            || AggregationFunctionCalciteType.isAggregationKindSupported(oldAggregation.getKind()),
        String.format("Unsupported SQL aggregation kind: %s. Only splittable aggregation functions are "
            + "supported! func name: %s", aggKind, oldAggregation.getName()));

    AggregateCall newCall;
    if (isLeafStageAggregationPresent) {
      newCall = AggregateCall.create(oldCall.getAggregation(), oldCall.isDistinct(), oldCall.isApproximate(),
          oldCall.ignoreNulls(), convertArgList(nGroups + oldCallIndex, oldCall.getArgList(),
              literalIndexMap),
          oldCall.filterArg, oldCall.distinctKeys, oldCall.collation, oldCall.type, oldCall.getName());
    } else {
      List<Integer> newArgList = oldCall.getArgList().size() == 0 ? Collections.emptyList()
          : Collections.singletonList(argList.get(oldCallIndex));

      newCall = AggregateCall.create(oldCall.getAggregation(), oldCall.isDistinct(), oldCall.isApproximate(),
          oldCall.ignoreNulls(), newArgList, oldCall.filterArg, oldCall.distinctKeys, oldCall.collation, oldCall.type,
          oldCall.getName());
    }

    rexBuilder.addAggCall(newCall, nGroups, newCalls, aggCallMapping, oldAggRel.getInput()::fieldIsNullable);
  }

  private static List<Integer> convertArgList(int oldCallIndexWithShift, List<Integer> argList,
      @Nullable Map<Integer, Integer> literalIndexMap) {
    //Preconditions.checkArgument(argList.size() <= 1,
    //    "Unable to convert call as the argList contains more than 1 argument");
    List<Integer> returnList = argList.size() == 0 ? Collections.emptyList() : new ArrayList<>();
    for (int i = 0; i < argList.size(); i++) {
      int indexInArgList = argList.get(i);
      if (literalIndexMap != null && literalIndexMap.containsKey(indexInArgList)) {
        returnList.add(literalIndexMap.get(indexInArgList));
      } else {
        returnList.add(oldCallIndexWithShift + i);
      }
    }
    return returnList;
  }

  private void createPlanWithoutLeafAggregation(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    RelNode childRel = ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
    LogicalProject project;

    List<Integer> newAggArgColumns = new ArrayList<>();
    List<Integer> newAggGroupByColumns = new ArrayList<>();

    // 1. Create the LogicalProject node if it does not exist. This is to send only the relevant columns over
    //    the wire for intermediate aggregation.
    if (childRel instanceof Project) {
      // Avoid creating a new LogicalProject if the child node of aggregation is already a project node.
      project = (LogicalProject) childRel;
      newAggArgColumns = fetchNewAggArgCols(oldAggRel.getAggCallList());
      newAggGroupByColumns = oldAggRel.getGroupSet().asList();
    } else {
      // Create a leaf stage project. This is done so that only the required columns are sent over the wire for
      // intermediate aggregation. If there are multiple aggregations on the same column, the column is projected
      // only once.
      project = createLogicalProjectForAggregate(oldAggRel, newAggArgColumns, newAggGroupByColumns);
    }

    // 2. Create an exchange on top of the LogicalProject.
    LogicalExchange exchange = LogicalExchange.create(project, RelDistributions.hash(newAggGroupByColumns));

    // 3. Create an intermediate stage aggregation.
    // TODO: Handle the scenario where we need to project literals if at all
    RelNode newAggNode =
        makeNewIntermediateAgg(call, oldAggRel, exchange, false, newAggArgColumns, newAggGroupByColumns, null);

    call.transformTo(newAggNode);
  }

  private LogicalProject createLogicalProjectForAggregate(Aggregate oldAggRel, List<Integer> newAggArgColumns,
      List<Integer> newAggGroupByCols) {
    RelNode childRel = ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
    RexBuilder childRexBuilder = childRel.getCluster().getRexBuilder();
    List<RelDataTypeField> fieldList = childRel.getRowType().getFieldList();

    List<RexNode> projectColRexNodes = new ArrayList<>();
    List<String> projectColNames = new ArrayList<>();
    // Maintains a mapping from the column to the corresponding index in projectColRexNodes.
    Map<Integer, Integer> projectSet = new HashMap<>();

    int projectIndex = 0;
    for (int group : oldAggRel.getGroupSet().asSet()) {
      projectColNames.add(fieldList.get(group).getName());
      projectColRexNodes.add(childRexBuilder.makeInputRef(childRel, group));
      projectSet.put(group, projectColRexNodes.size() - 1);
      newAggGroupByCols.add(projectIndex++);
    }

    List<AggregateCall> oldAggCallList = oldAggRel.getAggCallList();
    for (int i = 0; i < oldAggCallList.size(); i++) {
      List<Integer> argList = oldAggCallList.get(i).getArgList();
      if (argList.size() == 0) {
        newAggArgColumns.add(-1);
        continue;
      }
      for (int j = 0; j < argList.size(); j++) {
        Integer col = argList.get(j);
        if (!projectSet.containsKey(col)) {
          projectColRexNodes.add(childRexBuilder.makeInputRef(childRel, col));
          projectColNames.add(fieldList.get(col).getName());
          projectSet.put(col, projectColRexNodes.size() - 1);
          newAggArgColumns.add(projectColRexNodes.size() - 1);
        } else {
          newAggArgColumns.add(projectSet.get(col));
        }
      }
    }

    return LogicalProject.create(childRel, Collections.emptyList(), projectColRexNodes, projectColNames);
  }

  private List<Integer> fetchNewAggArgCols(List<AggregateCall> oldAggCallList) {
    List<Integer> newAggArgColumns = new ArrayList<>();

    for (int i = 0; i < oldAggCallList.size(); i++) {
      if (oldAggCallList.get(i).getArgList().size() == 0) {
        // This can be true for COUNT. Add a placeholder value which will be ignored.
        newAggArgColumns.add(-1);
        continue;
      }
      for (int j = 0; j < oldAggCallList.get(i).getArgList().size(); j++) {
        Integer col = oldAggCallList.get(i).getArgList().get(j);
        newAggArgColumns.add(col);
      }
    }

    return newAggArgColumns;
  }
}
