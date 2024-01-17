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
package org.apache.pinot.server.starter.helix;

import com.google.common.base.Preconditions;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now serving segment.
 * 3. Delete an existed segment.
 *
 * This only works for OFFLINE table segments and does not handle the CONSUMING state at all
 */
public class OfflineSegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private final String _instanceId;
  private final InstanceDataManager _instanceDataManager;

  public OfflineSegmentOnlineOfflineStateModelFactory(String instanceId, InstanceDataManager instanceDataManager) {
    _instanceId = instanceId;
    _instanceDataManager = instanceDataManager;
  }

  public static String getStateModelName() {
    return "OfflineSegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new OfflineSegmentOnlineOfflineStateModelFactory.OfflineSegmentOnlineOfflineStateModel();
  }


  // Helix seems to need StateModelInfo annotation for 'initialState'. It does not use the 'states' field.
  // The transitions in the helix messages indicate the from/to states, and helix uses the
  // Transition annotations (but only if StateModelInfo is defined).
  @SuppressWarnings("unused")
  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class OfflineSegmentOnlineOfflineStateModel extends StateModel {
    private final Logger _logger = LoggerFactory.getLogger(_instanceId + " - OfflineSegmentOnlineOfflineStateModel");

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      Preconditions.checkArgument((tableType != null) && (tableType != TableType.REALTIME),
          "TableType is null or is a REALTIME table, offline state model should not be called fo RT");
      try {
        _instanceDataManager.addOrReplaceSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        String errorMessage =
            String.format("Caught exception in state transition OFFLINE -> ONLINE for table: %s, segment: %s",
                tableNameWithType, segmentName);
        _logger.error(errorMessage, e);
        TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
        if (tableDataManager != null) {
          tableDataManager.addSegmentError(segmentName,
              new SegmentErrorInfo(System.currentTimeMillis(), errorMessage, e));
        }
        Utils.rethrowException(e);
      }
    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.offloadSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition ONLINE -> OFFLINE for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition OFFLINE -> DROPPED for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.offloadSegment(tableNameWithType, segmentName);
        _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition ONLINE -> DROPPED for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeOfflineFromError() : " + message);
    }

    @Transition(from = "ERROR", to = "DROPPED")
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      _logger.info("OfflineSegmentOnlineOfflineStateModel.onBecomeDroppedFromError() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition ERROR -> DROPPED for table: {}, segment: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }
  }
}
