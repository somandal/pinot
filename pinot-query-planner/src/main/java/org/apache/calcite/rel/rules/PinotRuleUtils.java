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

import java.util.List;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;


public class PinotRuleUtils {
  private static final RelBuilder.Config DEFAULT_CONFIG =
      RelBuilder.Config.DEFAULT.withAggregateUnique(true).withPushJoinCondition(true);

  public static final RelBuilderFactory PINOT_REL_FACTORY =
      RelBuilder.proto(Contexts.of(RelFactories.DEFAULT_STRUCT, DEFAULT_CONFIG));

  private PinotRuleUtils() {
    // do not instantiate.
  }

  public static boolean isExchange(RelNode rel) {
    RelNode reference = rel;
    if (reference instanceof HepRelVertex) {
      reference = ((HepRelVertex) reference).getCurrentRel();
    }
    return reference instanceof Exchange;
  }

  public static boolean isProject(RelNode rel) {
    RelNode reference = rel;
    if (reference instanceof HepRelVertex) {
      reference = ((HepRelVertex) reference).getCurrentRel();
    }
    return reference instanceof Project;
  }

  /**
   * Used by the ProjectWindowTranspose rules to find the references of the fields used which
   * should be added to the transposed project below the window
   */
  public static ImmutableBitSet findReference(final Project project,
      final Window window, boolean keepAtLeastOneReference) {
    final ImmutableBitSet.Builder beReferred = ImmutableBitSet.builder();
    final List<RelDataTypeField> rowTypeWindowInput = window.getInput().getRowType().getFieldList();
    final int windowInputColumn = window.getInput().getRowType().getFieldCount();

    final RexShuttle referenceFinder = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        final int index = inputRef.getIndex();
        if (index < windowInputColumn) {
          beReferred.set(index);
        }
        return inputRef;
      }
    };

    // Reference in LogicalProject
    referenceFinder.visitEach(project.getProjects());

    // Reference in LogicalWindow
    for (Window.Group group : window.groups) {
      // Reference in Partition-By
      for (int index : group.keys) {
        if (index < windowInputColumn) {
          beReferred.set(index);
        }
      }

      // Reference in Order-By
      for (RelFieldCollation relFieldCollation : group.orderKeys.getFieldCollations()) {
        if (relFieldCollation.getFieldIndex() < windowInputColumn) {
          beReferred.set(relFieldCollation.getFieldIndex());
        }
      }

      // Reference in Window Functions
      referenceFinder.visitEach(group.aggCalls);
    }

    if (beReferred.isEmpty() && keepAtLeastOneReference) {
      // No fields are referred but keep at least one field. For now picking the first
      // TODO: Assess whether we should select a specific type of column to keep such as NUMERIC only
      final RelDataTypeField relDataTypeField = rowTypeWindowInput.get(0);
      beReferred.set(relDataTypeField.getIndex());
    }

    return beReferred.build();
  }
}
