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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after WINDOW node.
 */
public class PinotWindowExchangeNodeInsertRule extends RelOptRule {
  public static final PinotWindowExchangeNodeInsertRule INSTANCE =
      new PinotWindowExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  private static final Set<SqlKind> SUPPORTED_AGG_KIND = ImmutableSet.of(SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN,
      SqlKind.MAX, SqlKind.COUNT, SqlKind.OTHER_FUNCTION);

  public PinotWindowExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalWindow.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Window) {
      Window window = call.rel(0);
      Window.Group group = window.groups.get(0);

      if (group.orderKeys.getKeys().isEmpty() && group.keys.isEmpty()) {
        // Empty OVER()
        // Only run the rule if the input isn't already an exchange node
        return !PinotRuleUtils.isExchange(window.getInput());
      } else if (!group.orderKeys.getKeys().isEmpty() || !group.keys.isEmpty()) {
        // All other window functions
        // Only run the rule if the input isn't already a sort node
        return !PinotRuleUtils.isSort(window.getInput());
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Window window = call.rel(0);
    RelNode windowInput = window.getInput();

    int numGroups = window.groups.size();

    // For Phase 1 we only handle single window groups
    Preconditions.checkState(numGroups <= 1, "Currently only 1 window group is supported");

    // Check only the supported AGG functions are provided for Phase 1
    for (int i = 0; i < window.groups.get(0).aggCalls.size(); i++) {
      Window.RexWinAggCall aggCall = window.groups.get(0).aggCalls.get(i);
      SqlKind aggKind = aggCall.getKind();
      Preconditions.checkState(SUPPORTED_AGG_KIND.contains(aggKind),
          "Unsupported Window function " + "kind: {}. Only aggregation functions are supported!", aggKind);
    }

    Window.Group windowGroup = window.groups.get(0);
    if (windowGroup.keys.isEmpty() && windowGroup.orderKeys.getKeys().isEmpty()) {
      // Empty OVER()

      // Add a single LogicalExchange for empty OVER()
      // Empty OVER() does not even need to be sorted
      LogicalExchange exchange = LogicalExchange.create(windowInput, RelDistributions.hash(Collections.emptyList()));
      call.transformTo(
          LogicalWindow.create(window.getTraitSet(), exchange, window.constants, window.getRowType(), window.groups));
    } else if (windowGroup.keys.isEmpty() && !windowGroup.orderKeys.getKeys().isEmpty()) {
      // Only ORDER BY

      // Add a sort and sort exchange below the window
      LogicalSortExchange exchange = LogicalSortExchange.create(windowInput,
          RelDistributions.hash(Collections.emptyList()), windowGroup.orderKeys);
      LogicalSort sort = LogicalSort.create(exchange, windowGroup.orderKeys, null, null);
      call.transformTo(LogicalWindow.create(window.getTraitSet(), sort, window.constants, window.getRowType(),
          window.groups));
    } else if (!windowGroup.keys.isEmpty() && windowGroup.orderKeys.getKeys().isEmpty()) {
      // Only PARTITION BY

      // Add a hash based exchange and logical sort under window
      List<RelFieldCollation> relFieldCollations = new ArrayList<>();
      List<Integer> windowGroupKeyList = windowGroup.keys.asList();
      for (Integer integer : windowGroupKeyList) {
        RelFieldCollation relFieldCollation = new RelFieldCollation(integer);
        relFieldCollations.add(relFieldCollation);
      }
      RelCollation relCollation = RelCollationImpl.of(relFieldCollations);
      LogicalSortExchange sortExchange = LogicalSortExchange.create(windowInput,
          RelDistributions.hash(windowGroup.keys.asList()), relCollation);
      // Sort should be based on the partition key
      LogicalSort sort = LogicalSort.create(sortExchange, relCollation, null, null);

      // New window
      LogicalWindow newWindow;
      newWindow = LogicalWindow.create(window.getTraitSet(), sort, window.constants, window.getRowType(),
          window.groups);

      RelOptCluster cluster = window.getCluster();
      List<RelDataTypeField> rowTypeWindowInput = window.getInput().getRowType().getFieldList();
      int windowInputColumn = rowTypeWindowInput.size();

      final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
      List<RexNode> exps = new ArrayList<>();
      for (int i = 0; i < windowInputColumn; i++) {
        RelDataTypeField relDataTypeField = window.getRowType().getFieldList().get(i);
        exps.add(new RexInputRef(i, relDataTypeField.getType()));
        builder.add(relDataTypeField);
      }
      for (int i = windowInputColumn; i < window.getRowType().getFieldCount(); i++) {
        RelDataTypeField relDataTypeField = window.getRowType().getFieldList().get(i);
        exps.add(new RexInputRef(i, relDataTypeField.getType()));
        builder.add(relDataTypeField);
      }
      LogicalProject curProject = LogicalProject.create(newWindow, Collections.emptyList(), exps, builder.build());

      // Build Global Sort + Sort Exchange + Project and add project just above window as child of this
      LogicalSortExchange sortExchangeUpper = LogicalSortExchange.create(curProject,
          RelDistributions.hash(Collections.emptyList()), relCollation);
      LogicalSort sortUpper = LogicalSort.create(sortExchangeUpper, relCollation, null, null);
      call.transformTo(sortUpper);
    } else {
      // Both PARTITION BY and ORDER BY

      // Add a hash based exchange and logical sort under window
      List<RelFieldCollation> relFieldCollations = new ArrayList<>();
      List<Integer> windowGroupKeyList = windowGroup.keys.asList();
      for (Integer integer : windowGroupKeyList) {
        RelFieldCollation relFieldCollation = new RelFieldCollation(integer);
        relFieldCollations.add(relFieldCollation);
      }
      // Exchange should only be based on partition by key
      RelCollation relCollation = RelCollationImpl.of(relFieldCollations);
      LogicalSortExchange sortExchange = LogicalSortExchange.create(windowInput,
          RelDistributions.hash(windowGroup.keys.asList()), relCollation);

      // Add order by keys to field collations list too
      relFieldCollations.addAll(windowGroup.orderKeys.getFieldCollations());
      RelCollation relCollationPartitionByAndOrderBy = RelCollationImpl.of(relFieldCollations);
      // Sort should be based on the partition by key and order by key
      LogicalSort sort = LogicalSort.create(sortExchange, relCollationPartitionByAndOrderBy, null, null);
      // New window
      LogicalWindow newWindow;
      newWindow = LogicalWindow.create(window.getTraitSet(), sort, window.constants, window.getRowType(),
          window.groups);

      RelOptCluster cluster = window.getCluster();
      List<RelDataTypeField> rowTypeWindowInput = window.getInput().getRowType().getFieldList();
      int windowInputColumn = rowTypeWindowInput.size();

      final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
      List<RexNode> exps = new ArrayList<>();
      for (int i = 0; i < windowInputColumn; i++) {
        RelDataTypeField relDataTypeField = window.getRowType().getFieldList().get(i);
        exps.add(new RexInputRef(i, relDataTypeField.getType()));
        builder.add(relDataTypeField);
      }
      for (int i = windowInputColumn; i < window.getRowType().getFieldCount(); i++) {
        RelDataTypeField relDataTypeField = window.getRowType().getFieldList().get(i);
        exps.add(new RexInputRef(i, relDataTypeField.getType()));
        builder.add(relDataTypeField);
      }
      LogicalProject curProject = LogicalProject.create(newWindow, Collections.emptyList(), exps, builder.build());

      // Build Global Sort + Sort Exchange + Project and add project just above window as child of this
      LogicalSortExchange sortExchangeUpper = LogicalSortExchange.create(curProject,
          RelDistributions.hash(Collections.emptyList()), relCollation);
      LogicalSort sortUpper = LogicalSort.create(sortExchangeUpper, relCollationPartitionByAndOrderBy, null, null);
      call.transformTo(sortUpper);
    }
  }
}
