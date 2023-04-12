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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;


/**
 * This rule extends the {@code ProjectWindowTransposeRule} provided by Apache Calcite to ignore
 * window functions that don't have any columns which can be added to the project transposed under
 * the window and allow handling such queries differently.
 */
@Value.Enclosing
public class ProjectWindowTransposeNonEmptyProjectRule extends ProjectWindowTransposeRule {
  public static final ProjectWindowTransposeNonEmptyProjectRule INSTANCE =
      new ProjectWindowTransposeNonEmptyProjectRule(PinotRuleUtils.PINOT_REL_FACTORY);

  protected ProjectWindowTransposeNonEmptyProjectRule(Config config) {
    super(config);
  }

  public ProjectWindowTransposeNonEmptyProjectRule(RelBuilderFactory relBuilderFactory) {
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
      return beReferred.asList().size() != 0;
    }
    return false;
  }
}
