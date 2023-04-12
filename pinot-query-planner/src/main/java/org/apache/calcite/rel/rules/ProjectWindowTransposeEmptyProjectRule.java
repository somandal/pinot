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
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;


/**
 * This rule extends the {@code ProjectWindowTransposeRule} provided by Apache Calcite to identify
 * window functions that don't have any columns which can be added to the project transposed under
 * the window. Example of such queries:
 *
 * SELECT COUNT(*) OVER() from table;
 * SELECT ROW_NUMBER() OVER() from table;
 *
 * The {@code ProjectWindowTransposeRule} has a bug where for such queries it creates an empty {@code LogicalProject}
 * node below the window and removes the upper {@code LogicalProject}. This is not the right thing to do as it results
 * in projecting nothing from the table scan node and the window has no data to apply the window functions to.
 *
 * This class solves this by always ensuring that at least one column is added to the project list. Most of the code
 * is copied from {@code ProjectWindowTransposeRule}.
 */
@Value.Enclosing
public class ProjectWindowTransposeEmptyProjectRule extends ProjectWindowTransposeRule {
  public static final ProjectWindowTransposeEmptyProjectRule INSTANCE =
      new ProjectWindowTransposeEmptyProjectRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public ProjectWindowTransposeEmptyProjectRule(RelBuilderFactory relBuilderFactory) {
    super(relBuilderFactory);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 2) {
      return false;
    }
    if (call.rel(0) instanceof Project && call.rel(1) instanceof Window) {
      final Project project = call.rel(0);
      final Window window = call.rel(1);
      final ImmutableBitSet beReferred = PinotRuleUtils.findReference(project, window, false);
      return (beReferred.asList().size() == 0) && !PinotRuleUtils.isProject(window.getInput());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Window window = call.rel(1);
    final RelOptCluster cluster = window.getCluster();
    final List<RelDataTypeField> rowTypeWindowInput =
        window.getInput().getRowType().getFieldList();
    final int windowInputColumn = rowTypeWindowInput.size();

    if (rowTypeWindowInput.size() == 0) {
      // Nothing to project as no inputs to window
      return;
    }

    // Record the window input columns which are actually referred
    // either in the LogicalProject above LogicalWindow or LogicalWindow itself
    // Here we try to keep at least one column to project for Window functions
    // that take no arguments such as COUNT(*) and ROW_NUMBER(). This is necessary
    // so that queries of the type 'SELECT COUNT(*) OVER() from table', which don't
    // reference any other columns, work.
    // (Note that the constants used in LogicalWindow are not considered here)
    final ImmutableBitSet beReferred = PinotRuleUtils.findReference(project, window, true);

    // Put a DrillProjectRel below LogicalWindow
    final List<RexNode> exps = new ArrayList<>();
    final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();

    // Keep only the fields which are referred
    for (int index : BitSets.toIter(beReferred)) {
      final RelDataTypeField relDataTypeField = rowTypeWindowInput.get(index);
      exps.add(new RexInputRef(index, relDataTypeField.getType()));
      builder.add(relDataTypeField);
    }

    final LogicalProject projectBelowWindow =
        new LogicalProject(cluster, window.getTraitSet(), ImmutableList.of(),
            window.getInput(), exps, builder.build());

    // Create a new LogicalWindow with necessary inputs only
    final List<Window.Group> groups = new ArrayList<>();

    // As the un-referred columns are trimmed by the LogicalProject,
    // the indices specified in LogicalWindow would need to be adjusted
    final RexShuttle indexAdjustment = new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef inputRef) {
        final int newIndex =
            getAdjustedIndex(inputRef.getIndex(), beReferred,
                windowInputColumn);
        return new RexInputRef(newIndex, inputRef.getType());
      }

      @Override
      public RexNode visitCall(final RexCall call) {
        if (call instanceof Window.RexWinAggCall) {
          final Window.RexWinAggCall aggCall = (Window.RexWinAggCall) call;
          boolean[] update = {false};
          final List<RexNode> clonedOperands = visitList(call.operands, update);
          if (update[0]) {
            return new Window.RexWinAggCall(
                (SqlAggFunction) call.getOperator(), call.getType(),
                clonedOperands, aggCall.ordinal, aggCall.distinct,
                aggCall.ignoreNulls);
          } else {
            return call;
          }
        } else {
          return super.visitCall(call);
        }
      }
    };

    int aggCallIndex = windowInputColumn;
    final RelDataTypeFactory.Builder outputBuilder =
        cluster.getTypeFactory().builder();
    outputBuilder.addAll(projectBelowWindow.getRowType().getFieldList());
    for (Window.Group group : window.groups) {
      final ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
      final List<RelFieldCollation> orderKeys = new ArrayList<>();
      final List<Window.RexWinAggCall> aggCalls = new ArrayList<>();

      // Adjust keys
      for (int index : group.keys) {
        keys.set(getAdjustedIndex(index, beReferred, windowInputColumn));
      }

      // Adjust orderKeys
      for (RelFieldCollation relFieldCollation : group.orderKeys.getFieldCollations()) {
        final int index = relFieldCollation.getFieldIndex();
        orderKeys.add(
            relFieldCollation.withFieldIndex(
                getAdjustedIndex(index, beReferred, windowInputColumn)));
      }

      // Adjust Window Functions
      for (Window.RexWinAggCall rexWinAggCall : group.aggCalls) {
        aggCalls.add((Window.RexWinAggCall) rexWinAggCall.accept(indexAdjustment));

        final RelDataTypeField relDataTypeField =
            window.getRowType().getFieldList().get(aggCallIndex);
        outputBuilder.add(relDataTypeField);
        ++aggCallIndex;
      }

      groups.add(
          new Window.Group(keys.build(), group.isRows, group.lowerBound,
              group.upperBound, RelCollations.of(orderKeys), aggCalls));
    }

    final LogicalWindow newLogicalWindow =
        LogicalWindow.create(window.getTraitSet(), projectBelowWindow,
            window.constants, outputBuilder.build(), groups);

    // Modify the top LogicalProject
    final List<RexNode> topProjExps =
        indexAdjustment.visitList(project.getProjects());

    final Project newTopProj = project.copy(
        newLogicalWindow.getTraitSet(),
        newLogicalWindow,
        topProjExps,
        project.getRowType());

    if (ProjectRemoveRule.isTrivial(newTopProj)) {
      call.transformTo(newLogicalWindow);
    } else {
      call.transformTo(newTopProj);
    }
  }

  private static int getAdjustedIndex(final int initIndex,
      final ImmutableBitSet beReferred, final int windowInputColumn) {
    if (initIndex >= windowInputColumn) {
      return beReferred.cardinality() + (initIndex - windowInputColumn);
    } else {
      return beReferred.get(0, initIndex).cardinality();
    }
  }
}
