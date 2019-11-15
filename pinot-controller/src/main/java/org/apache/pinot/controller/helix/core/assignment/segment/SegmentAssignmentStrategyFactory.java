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
package org.apache.pinot.controller.helix.core.assignment.segment;

import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Segment.AssignmentStrategy;


/**
 * Factory for the {@link SegmentAssignmentStrategy}.
 */
public class SegmentAssignmentStrategyFactory {
  private SegmentAssignmentStrategyFactory() {
  }

  public static SegmentAssignmentStrategy getSegmentAssignmentStrategy(HelixManager helixManager,
      TableConfig tableConfig) {
    SegmentAssignmentStrategy segmentAssignmentStrategy;
    if (AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY
        .equalsIgnoreCase(tableConfig.getValidationConfig().getSegmentAssignmentStrategy())) {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        segmentAssignmentStrategy = new OfflineReplicaGroupSegmentAssignmentStrategy();
      } else {
        segmentAssignmentStrategy = new RealtimeReplicaGroupSegmentAssignmentStrategy();
      }
    } else {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        segmentAssignmentStrategy = new OfflineBalanceNumSegmentAssignmentStrategy();
      } else {
        segmentAssignmentStrategy = new RealtimeBalanceNumSegmentAssignmentStrategy();
      }
    }
    segmentAssignmentStrategy.init(helixManager, tableConfig);
    return segmentAssignmentStrategy;
  }
}
