package org.apache.iotdb.db.queryengine.plan.execution;

import java.util.Map;
import java.util.HashMap;
public class PipeInfo {
    private static final PipeInfo instance=new PipeInfo();

    // 声明单例对象需要修改的属性
    private boolean pipeStatus;//pipe的启动状态 0：关闭  1：启动
    private Map<Integer, ScanStatusInfo> scanStatusInfos;
    private Map<Integer, JoinStatusInfo> joinStatusInfos;
    private int edge_rec_fragmentId;    // edge侧接收cloud端join后发来的数据时的fragment
    private int edge_send_fragmentId;   // edge侧用bloomfilter处理原始数据后向cloud发送数据的fragment
    private String sql;


    // 私有构造方法，避免外部实例化
    private PipeInfo() {
        this.pipeStatus=false;
        this.scanStatusInfos=new HashMap<>();
        this.edge_rec_fragmentId=1000;
        this.edge_send_fragmentId = 100000;
        this.joinStatusInfos = new HashMap<>();
    }

    // 提供获取实例的静态方法，使用 synchronized 关键字保证线程安全
    public static synchronized PipeInfo getInstance() {
        return instance;
    }

    // 设置单例对象的值
    public void setPipeStatus(boolean status){
        this.pipeStatus=status;
    }
    public void addScanSatus(int sourceId, int edgeSendFragmentId, int edgeRecFragment) {//添加算子
        ScanStatusInfo scanStatusInfo = new ScanStatusInfo(sourceId, edgeSendFragmentId,edgeRecFragment);
        scanStatusInfos.put(sourceId, scanStatusInfo);
    }
    public void addJoinSatus(int sourceId, int edgeSendFragmentId, int edgeRecFragment) {//添加算子
        JoinStatusInfo joinStatusInfo = new JoinStatusInfo(sourceId, edgeSendFragmentId,edgeRecFragment);
        joinStatusInfos.put(sourceId, joinStatusInfo);
    }

    // 获取单例对象的值
    public boolean getPipeStatus(){
        return  pipeStatus;
    }
    public ScanStatusInfo getScanStatus(int sourceId){
        return scanStatusInfos.get(sourceId);
    }
    public JoinStatusInfo getJoinStatus(int sourceId){
        return joinStatusInfos.get(sourceId);
    }
    public void printAllScanStatus() {
        for (Map.Entry<Integer, ScanStatusInfo> entry : scanStatusInfos.entrySet()) {
            int id = entry.getKey();
            OperatorStatusInfo operatorStatusInfo = entry.getValue();
            System.out.println("Source ID: " + id + ", Edge Fragment: " + operatorStatusInfo.getEdgeRecFragmentId() );
        }
    }
    public void printAllJoinStatus() {
        for (Map.Entry<Integer, JoinStatusInfo> entry : joinStatusInfos.entrySet()) {
            int id = entry.getKey();
            OperatorStatusInfo operatorStatusInfo = entry.getValue();
            System.out.println("Source ID: " + id + ", Edge Fragment: " + operatorStatusInfo.getEdgeRecFragmentId() );
        }
    }

    public int getRecFragmentId() {
        return edge_rec_fragmentId++;
    }
    public int getSendFragmentId() {
        return edge_send_fragmentId++;
    }

    public void clearAllScanStatus(){
//        this.scanStatusInfos=new HashMap<>();
        sql=null;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }


}
