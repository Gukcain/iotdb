package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;

public class OperatorStatusInfo {
  private int sourceId = 0;
  private int edgeFragmentId = 0;
  private int cloudFragmentId = 0;
  //    private int edgeRecFragmentId=0;    // egde-source-local / cloud-sink-remote
  //    private int cloudSendFragmentId=0;  // cloud-sink-local / edge-source-remote
  //    private int edgeSendFragmentId=0;   // edge-sink-local / cloud-source-remote
  //    private int cloudRecFragmentId=0;   // cloud-cource-local / edge-sink-remote
  private boolean status = false; // 是否传输数据
  private int offset = 0; // 算子偏移量
  private boolean setOffset = false;
  private boolean setStartTime = false;
  private int oldFragmentId = 0;
  private boolean hasNext = false;

  private long startTime = 0; // 起始时间
  private ISinkHandle EToCSinkHandle;

  OperatorStatusInfo(int sourceId, int edgeFragmentId) {
    this.edgeFragmentId = edgeFragmentId;
    this.sourceId = sourceId;
  }

  public void setEdgeFragmentId(int edgeFragmentId) {
    this.edgeFragmentId = edgeFragmentId;
  }

  public void setCloudFragmentId(int cloudFragmentId) {
    this.cloudFragmentId = cloudFragmentId;
  }

  public void setStatus(boolean status) {
    this.status = status;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public int getEdgeFragmentId() {
    return edgeFragmentId;
  }

  public int getCloudFragmentId() {
    return cloudFragmentId;
  }

  public boolean getStatus() {
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

  public void setOldFragmentId(int oldFragmentId) {
    this.oldFragmentId = oldFragmentId;
  }

  public int getOldFragmentId() {
    return oldFragmentId;
  }

  public void setStartTime(long time) {
    this.startTime = time;
  }

  public long getStartTime() {
    return startTime;
  }

  public boolean isSetStartTime() {
    return setStartTime;
  }

  public void setSetStartTime(boolean setStartTime) {
    this.setStartTime = setStartTime;
  }

  public ISinkHandle getEToCSinkHandle() {
    return EToCSinkHandle;
  }

  public void setEToCSinkHandle(ISinkHandle EToCSinkHandle) {
    this.EToCSinkHandle = EToCSinkHandle;
  }

  public boolean isHasNext() {
    return hasNext;
  }

  public void setHasNext(boolean hasNext) {
    this.hasNext = hasNext;
  }
}
