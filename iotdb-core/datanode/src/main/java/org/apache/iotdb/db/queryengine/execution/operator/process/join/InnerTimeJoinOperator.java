/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.successfulAsList;

public class InnerTimeJoinOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  /** Start index for each input TsBlocks and size of it is equal to inputTsBlocks. */
  private final int[] inputIndex;

  private final List<Operator> children;
  private final int inputOperatorsCount;
  /** TsBlock from child operator. Only one cache now. */
  private final TsBlock[] inputTsBlocks;

  private final boolean[] canCallNext;

  private final TsBlockBuilder resultBuilder;

  private final TimeComparator comparator;

  private final Map<InputLocation, Integer> outputColumnMap;

  /** Index of the child that is currently fetching input */
  private int currentChildIndex = 0;

  /** Indicate whether we found an empty child input in one loop */
  private boolean hasEmptyChildInput = false;

  private boolean finished;
  private PlanNodeId localPlanNode;
  private int edgeFragmentId; // remote
  private int offset = 0;
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private ISourceHandle sourceHandle;
  private int testflag = 0;
  private int cloudFragmentId = 0; // local
  private ISinkHandle sinkHandle;

  public InnerTimeJoinOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      List<TSDataType> dataTypes,
      TimeComparator comparator,
      Map<InputLocation, Integer> outputColumnMap) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.canCallNext = new boolean[inputOperatorsCount];
    checkArgument(
        children.size() > 1, "child size of InnerTimeJoinOperator should be larger than 1");
    this.inputIndex = new int[this.inputOperatorsCount];
    this.resultBuilder = new TsBlockBuilder(dataTypes);
    this.comparator = comparator;
    this.outputColumnMap = outputColumnMap;
  }

  public InnerTimeJoinOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      List<TSDataType> dataTypes,
      TimeComparator comparator,
      Map<InputLocation, Integer> outputColumnMap,
      int fragmentId) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.canCallNext = new boolean[inputOperatorsCount];
    checkArgument(
        children.size() > 1, "child size of InnerTimeJoinOperator should be larger than 1");
    this.inputIndex = new int[this.inputOperatorsCount];
    this.resultBuilder = new TsBlockBuilder(dataTypes);
    this.comparator = comparator;
    this.outputColumnMap = outputColumnMap;
    this.cloudFragmentId = fragmentId;
    this.localPlanNode = operatorContext.getPlanNodeId();

    createSourceHandle();
    createSinkHandle();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isEmpty(i)) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        hasReadyChild = true;
        canCallNext[i] = true;
      } else {
        listenableFutures.add(blocked);
      }
    }
    return (hasReadyChild || listenableFutures.isEmpty())
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  public void createSourceHandle() {
    // source
    System.out.println("---in---");
    System.out.println("---cloudLocalFragmentid:" + cloudFragmentId);
    System.out.println(
        "remote:"
            + PipeInfo.getInstance()
                .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
                .getEdgeFragmentId());
    String queryId = "test_query_" + localPlanNode.getId();
    TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
    while (PipeInfo.getInstance()
                .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
                .getEdgeFragmentId()
            == PipeInfo.getInstance()
                .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
                .getOldFragmentId()
        && PipeInfo.getInstance()
            .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
            .getStatus()) {
      try { // 如果这次和上次的edge fragmentId一样说明此时新的edge fragmentId还没发送过来 等待
        Thread.sleep(10);
        System.out.println("waiting");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    edgeFragmentId =
        PipeInfo.getInstance()
            .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
            .getEdgeFragmentId();
    TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(queryId, edgeFragmentId, "0");
    String localPlanNodeId = "receive_test_" + localPlanNode.getId();
    TFragmentInstanceId localFragmentInstanceId =
        new TFragmentInstanceId(queryId, cloudFragmentId, "0");
    long query_num = 1;
    FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
    System.out.println("edgeid:" + edgeFragmentId);
    this.sourceHandle =
        MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
            localFragmentInstanceId,
            localPlanNodeId,
            0, // IndexOfUpstreamSinkHandle
            remoteEndpoint,
            remoteFragmentInstanceId,
            instanceContext::failed);
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    sourceHandle.setMaxBytesCanReserve(MOCK_TSBLOCK_SIZE);
  }

  public void createSinkHandle() {
    // sink
    final String queryId = "test_query_" + localPlanNode.getId(); // 一个查询一个id就可以
    final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
    final TFragmentInstanceId remoteFragmentInstanceId =
        new TFragmentInstanceId(
            queryId,
            PipeInfo.getInstance()
                .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
                .getEdgeFragmentId(),
            "0"); // fragmentId是int变量 local和remote对应上即可
    final String remotePlanNodeId = "receive_test_" + localPlanNode.getId();
    final String localPlanNodeId = "send_test_" + localPlanNode.getId();
    final TFragmentInstanceId localFragmentInstanceId =
        new TFragmentInstanceId(queryId, cloudFragmentId, "0");
    int channelNum = 1;
    long query_num = 1;
    FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
    DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
    sinkHandle =
        MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    remoteEndpoint, remoteFragmentInstanceId, remotePlanNodeId)),
            downStreamChannelIndex,
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            instanceContext);
    PipeInfo.getInstance()
        .getJoinStatus(Integer.parseInt(localPlanNode.getId()))
        .setCToESinkHandle(this.sinkHandle);
    if (PipeInfo.getInstance().getPipeStatus()) {
      sinkHandle.tryOpenChannel(0); // 打开通道
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if(PipeInfo.getInstance().getPipeStatus()&&PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getStatus()){
//      Column[] valueColumns = resultTsBlock.getValueColumns();
//      System.out.println("result columns binary:");
//      Binary[] binaryColumn=valueColumns[0].getBinaries();
//      for(Binary binaryObject:binaryColumn){
//        System.out.println(binaryObject);
//      }
//      TimeColumn timeColumn=resultTsBlock.getTimeColumn();
//      long[] times=timeColumn.getTimes();
//      System.out.println("result time columns:");
//      for(long time:times){
//        System.out.println(time);
//      }
      System.out.println("localfragmentid:"+cloudFragmentId);
      System.out.println("remoteid:"+PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getEdgeFragmentId());

      ListenableFuture<?> isBlocked = sourceHandle.isBlocked();
      while (!isBlocked.isDone()&&!sourceHandle.isFinished()) {
        try {
          Thread.sleep(10);
          System.out.println("waiting");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      TsBlock tsBlock_rev = null;
      int rec_num = 0;
      while(!sourceHandle.isFinished()&&rec_num<=inputTsBlocks.length){
//      if (!sourceHandle.isFinished()) {
        tsBlock_rev = sourceHandle.receive();     // Exception: SourceHandle is blocked
        try (FileWriter writer = new FileWriter("HandleTest.txt", true)) {
          writer.write("----[Cloud]Receive TsBlock: "+ tsBlock_rev.getPositionCount() + "\n");  // 将字符串写入文件
          System.out.println("----[Cloud]Receive TsBlock: "+ tsBlock_rev.getPositionCount());
        } catch (IOException e) {
          System.out.println("发生错误：" + e.getMessage());
        }
        inputTsBlocks[rec_num] = tsBlock_rev;
        rec_num++;
      }
      if(rec_num!=inputTsBlocks.length){
        System.out.println("Error! Cloud Join-Node Receives "+ rec_num +" Blocks While InputTsBlocks Size = " + inputTsBlocks.length);
      }
//      System.out.println("isNoMoreTs:"+ sinkHandle.getChannel(0).isNoMoreTsBlocks());

//      sinkHandle.wait();
//      while(sinkHandle.getChannel(0).getNumOfBufferedTsBlocks()!=0){
//        try {
//                Thread.sleep(10);
//        //          System.out.println("waiting");
//              } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//              }
//      }

    }
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    if (!prepareInput(start, maxRuntime)) {
      return null;
    }

    // still have time
    if (System.nanoTime() - start < maxRuntime) {
      // End time for returned TsBlock this time, it's the min/max end time among all the children
      // TsBlocks order by asc/desc
      long currentEndTime = 0;
      boolean init = false;

      // Get TsBlock for each input, put their time stamp into TimeSelector and then use the min
      // Time
      // among all the input TsBlock as the current output TsBlock's endTime.
      for (int i = 0; i < inputOperatorsCount; i++) {
        // Update the currentEndTime if the TsBlock is not empty
        currentEndTime =
            init
                ? comparator.getCurrentEndTime(currentEndTime, inputTsBlocks[i].getEndTime())
                : inputTsBlocks[i].getEndTime();
        init = true;
      }

      // collect time that each child has
      int[][] selectedRowIndexArray = buildTimeColumn(currentEndTime);

      // build value columns for each child
      if (selectedRowIndexArray[0].length > 0) {
        for (int i = 0; i < inputOperatorsCount; i++) {
          buildValueColumns(i, selectedRowIndexArray[i]);
        }
      }
    }

    // set corresponding inputTsBlock to null if its index already reach its size, friendly for gc
    cleanUpInputTsBlock();

    TsBlock res = resultBuilder.build();
    if(PipeInfo.getInstance().getPipeStatus()&&PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getStatus())
      sinkHandle.send(res);//发送数据
    try (FileWriter writer = new FileWriter("HandleTest.txt", true)) {
      writer.write("----[Cloud]Send TsBlock: "+ res.getPositionCount() + "\n");  // 将字符串写入文件
      System.out.println("----[Cloud]Send TsBlock: "+ res.getPositionCount());
    } catch (IOException e) {
      System.out.println("发生错误：" + e.getMessage());
    }
    resultBuilder.reset();
    return res;
  }

  // return selected row index for each child's tsblock
  private int[][] buildTimeColumn(long currentEndTime) {
    TimeColumnBuilder timeBuilder = resultBuilder.getTimeColumnBuilder();
    List<List<Integer>> selectedRowIndexArray = new ArrayList<>(inputOperatorsCount);
    for (int i = 0; i < inputOperatorsCount; i++) {
      selectedRowIndexArray.add(new ArrayList<>());
    }

    int column0Size = inputTsBlocks[0].getPositionCount();

    while (inputIndex[0] < column0Size
        && comparator.canContinueInclusive(
            inputTsBlocks[0].getTimeByIndex(inputIndex[0]), currentEndTime)) {
      long time = inputTsBlocks[0].getTimeByIndex(inputIndex[0]);
      inputIndex[0]++;
      boolean allHave = true;
      for (int i = 1; i < inputOperatorsCount; i++) {
        int size = inputTsBlocks[i].getPositionCount();
        updateInputIndex(i, time);

        if (inputIndex[i] == size || inputTsBlocks[i].getTimeByIndex(inputIndex[i]) != time) {
          allHave = false;
          break;
        } else {
          inputIndex[i]++;
        }
      }
      if (allHave) {
        timeBuilder.writeLong(time);
        resultBuilder.declarePosition();
        appendOneSelectedRow(selectedRowIndexArray);
      }
    }

    // update inputIndex for each child to the last index larger than currentEndTime
    for (int i = 0; i < inputOperatorsCount; i++) {
      updateInputIndexUntilLargerThan(i, currentEndTime);
    }

    return transformListToIntArray(selectedRowIndexArray);
  }

  private void appendOneSelectedRow(List<List<Integer>> selectedRowIndexArray) {
    for (int i = 0; i < inputOperatorsCount; i++) {
      selectedRowIndexArray.get(i).add(inputIndex[i] - 1);
    }
  }

  private void updateInputIndex(int i, long currentEndTime) {
    int size = inputTsBlocks[i].getPositionCount();
    while (inputIndex[i] < size
        && comparator.lessThan(inputTsBlocks[i].getTimeByIndex(inputIndex[i]), currentEndTime)) {
      inputIndex[i]++;
    }
  }

  private void updateInputIndexUntilLargerThan(int i, long currentEndTime) {
    int size = inputTsBlocks[i].getPositionCount();
    while (inputIndex[i] < size
        && comparator.canContinueInclusive(
            inputTsBlocks[i].getTimeByIndex(inputIndex[i]), currentEndTime)) {
      inputIndex[i]++;
    }
  }

  private void cleanUpInputTsBlock() {
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (inputTsBlocks[i].getPositionCount() == inputIndex[i]) {
        inputTsBlocks[i] = null;
        inputIndex[i] = 0;
      }
    }
  }

  private int[][] transformListToIntArray(List<List<Integer>> lists) {
    if (lists.size() <= 1) {
      throw new IllegalStateException(
          "Child size of InnerTimeJoinOperator should be larger than 1.");
    }
    int[][] res = new int[lists.size()][lists.get(0).size()];
    for (int i = 0; i < res.length; i++) {
      List<Integer> list = lists.get(i);
      int[] array = res[i];
      if (list.size() != array.length) {
        throw new IllegalStateException("All child should have same time column result!");
      }
      for (int j = 0; j < array.length; j++) {
        array[j] = list.get(j);
      }
    }
    return res;
  }

  private void buildValueColumns(int childIndex, int[] selectedRowIndex) {
    TsBlock tsBlock = inputTsBlocks[childIndex];
    for (int i = 0, size = inputTsBlocks[childIndex].getValueColumnCount(); i < size; i++) {
      ColumnBuilder columnBuilder =
          resultBuilder.getColumnBuilder(outputColumnMap.get(new InputLocation(childIndex, i)));
      Column column = tsBlock.getColumn(i);
      if (column.mayHaveNull()) {
        for (int rowIndex : selectedRowIndex) {
          if (column.isNull(rowIndex)) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.write(column, rowIndex);
          }
        }
      } else {
        for (int rowIndex : selectedRowIndex) {
          columnBuilder.write(column, rowIndex);
        }
      }
    }
  }

  /**
   * Try to cache one result of each child.
   *
   * @return true if results of all children are ready. Return false if some children is blocked or
   *     return null.
   * @throws Exception errors happened while getting tsblock from children
   */
  private boolean prepareInput(long start, long maxRuntime) throws Exception {
    while (System.nanoTime() - start < maxRuntime && currentChildIndex < inputOperatorsCount) {
      if (!isEmpty(currentChildIndex)) {
        currentChildIndex++;
        continue;
      }
      if (canCallNext[currentChildIndex]) {
        if (children.get(currentChildIndex).hasNextWithTimer()) {
          inputIndex[currentChildIndex] = 0;
          inputTsBlocks[currentChildIndex] = children.get(currentChildIndex).nextWithTimer();
          canCallNext[currentChildIndex] = false;
          // child operator has next but return an empty TsBlock which means that it may not
          // finish calculation in given time slice.
          // In such case, TimeJoinOperator can't go on calculating, so we just return null.
          // We can also use the while loop here to continuously call the hasNext() and next()
          // methods of the child operator until its hasNext() returns false or the next() gets
          // the data that is not empty, but this will cause the execution time of the while loop
          // to be uncontrollable and may exceed all allocated time slice
          if (isEmpty(currentChildIndex)) {
            hasEmptyChildInput = true;
          }
        } else {
          return false;
        }
      } else {
        hasEmptyChildInput = true;
      }
      currentChildIndex++;
    }

    if (currentChildIndex == inputOperatorsCount) {
      // start a new loop
      currentChildIndex = 0;
      if (!hasEmptyChildInput) {
        // all children are ready now
        return true;
      } else {
        // In a new loop, previously empty child input could be non-empty now, and we can skip the
        // children that have generated input
        hasEmptyChildInput = false;
      }
    }
    return false;
  }

  @Override
  public boolean hasNext() throws Exception {

    if (PipeInfo.getInstance().getPipeStatus()&&PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getStatus())
      return true;   // TODO: 正确性？

    // return false if any child is consumed up.
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (isEmpty(i) && canCallNext[i] && !children.get(i).hasNextWithTimer()) {
        return false;
      }
    }
    // return true if all children still hava data
    return true;
  }

  @Override
  public void close() throws Exception {
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (children.get(i) != null) {
        children.get(i).close();
      }
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    // return true if any child is finished.
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (isEmpty(i) && children.get(i).isFinished()) {
        return true;
      }
    }
    // return false if all children still hava data
    return false;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory =
          Math.max(
              childrenMaxPeekMemory, maxPeekMemory + child.calculateMaxPeekMemoryWithCounter());
      maxPeekMemory +=
          (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }

    maxPeekMemory += calculateMaxReturnSize();
    return Math.max(maxPeekMemory, childrenMaxPeekMemory);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0;
    long minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long tmpMaxReturnSize = child.calculateMaxReturnSize();
      currentRetainedSize += (tmpMaxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, tmpMaxReturnSize);
    }
    // max cached TsBlock
    return currentRetainedSize - minChildReturnSize;
  }

  /**
   * If the tsBlock of columnIndex is null or has no more data in the tsBlock, return true; else
   * return false.
   */
  protected boolean isEmpty(int columnIndex) {
    return inputTsBlocks[columnIndex] == null
        || inputTsBlocks[columnIndex].getPositionCount() == inputIndex[columnIndex];
  }
}
