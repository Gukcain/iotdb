package org.apache.iotdb.db.queryengine.plan.execution;

public class JoinStatusInfo extends OperatorStatusInfo{

    JoinStatusInfo(int sourceId, int edgeSendFragmentId, int edgeRecFragmentId) {
        super(sourceId, edgeSendFragmentId, edgeRecFragmentId);
    }
}
