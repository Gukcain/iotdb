package org.apache.iotdb.db.queryengine.plan.execution;

public class JoinStatusInfo extends OperatorStatusInfo {

  JoinStatusInfo(int sourceId, int edgeFragmentId) {
    super(sourceId, edgeFragmentId);
  }
}
