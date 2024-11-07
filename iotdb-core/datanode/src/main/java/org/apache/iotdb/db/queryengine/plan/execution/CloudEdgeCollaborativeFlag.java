package org.apache.iotdb.db.queryengine.plan.execution;

/** ReadFromCloudFlag in SeriesScanOperator Collaborative */
public class CloudEdgeCollaborativeFlag {
  private static CloudEdgeCollaborativeFlag instance;
  public boolean cloudEdgeCollaborativeFlag;

  private CloudEdgeCollaborativeFlag() {
    //        cloudEdgeCollaborativeFlag = false;
    cloudEdgeCollaborativeFlag = true;
  }

  public void setFlag(boolean value) {
    cloudEdgeCollaborativeFlag = value;
  }

  public static CloudEdgeCollaborativeFlag getInstance() {
    if (instance == null) {
      instance = new CloudEdgeCollaborativeFlag();
    }
    return instance;
  }
}
