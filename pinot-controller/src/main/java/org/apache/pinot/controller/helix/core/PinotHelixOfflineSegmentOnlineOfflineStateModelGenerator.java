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
package org.apache.pinot.controller.helix.core;

import org.apache.helix.model.StateModelDefinition;

/**
 * Offline Segment state model generator describes the transitions for offline segment states.
 *
 * Online to Offline, Online to Dropped
 * Offline to Online, Offline to Dropped
 *
 * This does not include the state transitions for realtime segments (which includes the CONSUMING state)
 */
public class PinotHelixOfflineSegmentOnlineOfflineStateModelGenerator {
  private PinotHelixOfflineSegmentOnlineOfflineStateModelGenerator() {
  }

  public static final String PINOT_OFFLINE_SEGMENT_ONLINE_OFFLINE_STATE_MODEL = "OfflineSegmentOnlineOfflineStateModel";

  public static final String ONLINE_STATE = "ONLINE";
  public static final String OFFLINE_STATE = "OFFLINE";
  public static final String DROPPED_STATE = "DROPPED";

  public static StateModelDefinition generatePinotStateModelDefinition() {
    StateModelDefinition.Builder builder =
        new StateModelDefinition.Builder(PINOT_OFFLINE_SEGMENT_ONLINE_OFFLINE_STATE_MODEL);
    builder.initialState(OFFLINE_STATE);

    builder.addState(ONLINE_STATE);
    builder.addState(OFFLINE_STATE);
    builder.addState(DROPPED_STATE);
    // Set the initial state when the node starts

    // Add transitions between the states.
    builder.addTransition(OFFLINE_STATE, ONLINE_STATE);
    builder.addTransition(ONLINE_STATE, OFFLINE_STATE);
    builder.addTransition(OFFLINE_STATE, DROPPED_STATE);

    // set constraints on states.
    // static constraint
    builder.dynamicUpperBound(ONLINE_STATE, "R");
    // dynamic constraint, R means it should be derived based on the replication
    // factor.

    StateModelDefinition statemodelDefinition = builder.build();
    return statemodelDefinition;
  }
}
