package org.apache.iotdb.db.queryengine.plan.execution;

public class ScanStatusInfo extends OperatorStatusInfo{

    ScanStatusInfo(int sourceId, int edgeSendFragmentId, int edgeRecFragmentId) {
        super(sourceId, edgeSendFragmentId, edgeRecFragmentId);
    }
}
