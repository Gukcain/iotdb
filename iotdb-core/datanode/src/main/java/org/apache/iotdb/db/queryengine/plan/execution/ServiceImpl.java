package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.zcy.service.PipeCtoEService;

import org.apache.thrift.TException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ServiceImpl implements PipeCtoEService.Iface {

  @Override
  public void PipeStart(String sql) throws TException {
    System.out.println("[" + System.currentTimeMillis() + "]," + "Thread ID: " + Thread.currentThread().getId() + ", Enter PipeStart.");
    Thread thread = new Thread(new ExcuteSqlRunnable(sql)); // 发送数据测试
    thread.start();
    PipeInfo pipeInfo = PipeInfo.getInstance(); // 设置pipe状态为启动
    pipeInfo.setPipeStatus(true);
    pipeInfo.setCollaborationFlag(true);


    // 创建控制线程
    Thread controlThread = new Thread(() -> {
      while (pipeInfo.getPipeStatus()) {
        try {
          Thread.sleep(1000); // 定期检查 pipeStatus 状态
        } catch (InterruptedException e) {
          System.out.println("Control thread interrupted.");
          Thread.currentThread().interrupt();
          break;
        }
      }

      // 当 pipeStatus 为 false 时，关闭工作线程
      pipeInfo.setCollaborationFlag(false);
      thread.interrupt(); // 中断工作线程
      System.out.println("[" + System.currentTimeMillis() + "]," + "Thread ID: " + Thread.currentThread().getId() + ", Interrupt Pipe.");
      System.out.println("PipeStatus is false. Worker thread interrupted.");
    });

    controlThread.start(); // 启动控制线程
  }

  @Override
  public void AnsMessage(int EdgeFragmentId, int SourceId, int ReadOffset) throws TException {
    System.out.println("[" + System.currentTimeMillis() + "]," + "Thread ID: " + Thread.currentThread().getId() + ", AnsMessage.");
    PipeInfo pipeInfo = PipeInfo.getInstance();
//    pipeInfo.getScanStatus(SourceId).setEdgeFragmentId(EdgeFragmentId);
    pipeInfo.getJoinStatus(SourceId).setOffset(ReadOffset);

    pipeInfo.setQueryID(ReadOffset);    // 这里的readoffset发送的是queryid，未改参数名
//    pipeInfo.getScanStatus(SourceId).setStatus(true);
    pipeInfo.getJoinStatus(SourceId).setEdgeFragmentId(EdgeFragmentId);
    System.out.println("[Ans] --EdgeFragmentId:" + EdgeFragmentId);
//    pipeInfo.getJoinStatus(SourceId).setHasNext(hasNext);
    pipeInfo.getJoinStatus(SourceId).setStatus(true);
    pipeInfo.getJoinStatus(SourceId).setSetOffset(true);
  }

  public void AnsMessage(int EdgeFragmentId, int SourceId, boolean hasNext) throws TException {
    PipeInfo pipeInfo = PipeInfo.getInstance();
    pipeInfo.getJoinStatus(SourceId).setEdgeFragmentId(EdgeFragmentId);
    pipeInfo.getJoinStatus(SourceId).setHasNext(hasNext);
    pipeInfo.getJoinStatus(SourceId).setStatus(true);
  }

  @Override
  public void AnsAggreMessage(int EdgeFragmentId, int SourceId, long StartTime) throws TException {
    PipeInfo pipeInfo = PipeInfo.getInstance();
    pipeInfo.getScanStatus(SourceId).setEdgeFragmentId(EdgeFragmentId);
    pipeInfo.getScanStatus(SourceId).setStartTime(StartTime);
    pipeInfo.getScanStatus(SourceId).setStatus(true);
  }

  @Override
  public void PipeClose() throws TException {
    PipeInfo pipeInfo = PipeInfo.getInstance();
//    pipeInfo.closeAllJoinStatus();
////    pipeInfo.setPipeStatus(false);
//    pipeInfo.clearAllJoinStatus();
//    while(!pipeInfo.getJoinStatusInfos().isEmpty()){
//        try {
//          Thread.sleep(10);
//          System.out.println("Waiting for JoinStatusInfos close and clear.");
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }
    pipeInfo.setPipeStatus(false);

  }
}

class ExcuteSqlRunnable implements Runnable {
  private final String sql;

  public ExcuteSqlRunnable(String sql) {
    this.sql = sql;
  }

  @Override
  public void run() {
    while(sql==null){
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    System.out.println("[" + System.currentTimeMillis() + "]," + "Thread ID: " + Thread.currentThread().getId() + ", Start SQL.");
    System.out.println("Start sql \""+ sql + "\"");
    ClientRPCServiceImpl clientRPCService = new ClientRPCServiceImpl();
    clientRPCService.executeIdentitySql(sql);
    System.out.println("End sql \""+ sql + "\"");
  }
}
