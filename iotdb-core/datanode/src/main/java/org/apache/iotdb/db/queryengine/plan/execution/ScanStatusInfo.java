package org.apache.iotdb.db.queryengine.plan.execution;

public class ScanStatusInfo extends OperatorStatusInfo {

  ScanStatusInfo(int sourceId, int edgeFragmentId) {
    super(sourceId, edgeFragmentId);
  }
}
