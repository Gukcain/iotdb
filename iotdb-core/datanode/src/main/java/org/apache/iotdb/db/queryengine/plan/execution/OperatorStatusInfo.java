package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;

public class OperatorStatusInfo {
    private int sourceId=0;
    private int edgeRecFragmentId=0;    // egde-source-local / cloud-sink-remote
    private int cloudSendFragmentId=0;  // cloud-sink-local / edge-source-remote
    private int edgeSendFragmentId=0;   // edge-sink-local / cloud-source-remote
    private int cloudRecFragmentId=0;   // cloud-cource-local / edge-sink-remote
    private boolean status=false;//是否传输数据
    private int offset=0;//算子偏移量
    private boolean setOffset=false;
    private boolean setStartTime=false;
    private int oldFragmentId=0;

    private long startTime=0;//起始时间
    private ISinkHandle sinkHandle;


    OperatorStatusInfo(int sourceId, int edgeSendFragmentId,int edgeRecFragmentId){
        this.edgeSendFragmentId=edgeSendFragmentId;
        this.sourceId=sourceId;
        this.edgeRecFragmentId=edgeRecFragmentId;
    }

    public void setEdgeSendFragmentId(int edgeSendFragmentId) {
        this.edgeSendFragmentId = edgeSendFragmentId;
    }
    public void setEdgeRecFragmentId(int edgeRecFragmentId) {
        this.edgeRecFragmentId = edgeRecFragmentId;
    }

    public void setCloudRecFragmentId(int cloudRecFragmentId) {
        this.cloudRecFragmentId = cloudRecFragmentId;
    }
    public void setCloudSendFragmentId(int cloudSendFragmentId) {
        this.cloudSendFragmentId = cloudSendFragmentId;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }


    public int getCloudSendFragmentId() {
        return cloudSendFragmentId;
    }
    public int getCloudRecFragmentId() {
        return cloudRecFragmentId;
    }
    public int getEdgeSendFragmentId() {
        return edgeSendFragmentId;
    }
    public int getEdgeRecFragmentId() {
        return edgeRecFragmentId;
    }

    public boolean isStatus() {
        return status;
    }

    public int getOffset() {
        return offset;
    }

    public boolean isSetOffset() {
        return setOffset;
    }

    public void setSetOffset(boolean setOffset) {
        this.setOffset = setOffset;
    }

    public void setOldFragmentId(int oldFragmentId){this.oldFragmentId=oldFragmentId;}
    public int getOldFragmentId(){return oldFragmentId;}

    public void setStartTime(long time){this.startTime=time;}

    public long getStartTime(){return startTime;}

    public boolean isSetStartTime() {
        return setStartTime;
    }
    public void setSetStartTime(boolean setStartTime) {
        this.setStartTime = setStartTime;
    }

    public ISinkHandle getSinkHandle() {
        return sinkHandle;
    }

    public void setSinkHandle(ISinkHandle sinkHandle) {
        this.sinkHandle = sinkHandle;
    }

}
