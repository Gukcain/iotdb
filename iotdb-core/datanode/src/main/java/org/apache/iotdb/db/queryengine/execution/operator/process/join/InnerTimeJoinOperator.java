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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.plan.execution.CloudEdgeCollaborativeFlag;
import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.ReceiveTsBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.BloomFilter;

import com.google.common.util.concurrent.ListenableFuture;

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

  private boolean flag_boolean=false;
  private boolean flag_binary=false;
  private static TsBlock rev_tsblock=null;
  private static Boolean Isrev = false;
  private static Boolean finish_rev =false;
  private static boolean finish_boolean=false;
  private static boolean finish_binary=false;

  private PlanNodeId localPlanNode;
  private int edge_rec_fragmentId;   // to receive remote fragement
  private int offset=0;
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private ISourceHandle sourceHandle;
//  private SeriesScanOptions.Builder seriesScanBuilder;
//  private PartialPath seriesPath;
//  private Ordering scanOrder;
  private int testflag=0;
  private int CloudFragmentId=0;
  private int edge_send_fragmentId;  // to send fragment(block processed with bloomfilter) from
//  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
//          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
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
          int rec_fragmentId,
          int send_fragment) {
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
    this.edge_rec_fragmentId = rec_fragmentId;
    this.edge_send_fragmentId = send_fragment;
    this.localPlanNode = operatorContext.getPlanNodeId();
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

  public void createSourceHandle(){
    // source
    System.out.println("---in---");
    System.out.println("---localfragmentid:"+edge_rec_fragmentId);
    System.out.println("remote:"+PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getCloudSendFragmentId());
    String queryId = "test_query_"+localPlanNode.getId();
    TEndPoint remoteEndpoint = new TEndPoint("localhost", 10740);
    TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getCloudSendFragmentId(), "0");
    String localPlanNodeId = "receive_test_"+localPlanNode.getId();
    TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, edge_rec_fragmentId, "0");
    long query_num=1;
    FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
    System.out.println("cloudid:"+PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getCloudSendFragmentId());
    this.sourceHandle =MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
            localFragmentInstanceId,
            localPlanNodeId,
            0,//IndexOfUpstreamSinkHandle
            remoteEndpoint,
            remoteFragmentInstanceId,
            instanceContext::failed);
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    sourceHandle.setMaxBytesCanReserve(MOCK_TSBLOCK_SIZE);
  }

  public void createSinkHandle(){
    // sink
    final String queryId = "test_query_"+localPlanNode.getId();//一个查询一个id就可以
    final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).getCloudRecFragmentId(), "0");//fragmentId是int变量 local和remote对应上即可
    final String remotePlanNodeId = "receive_test_"+localPlanNode.getId();
    final String localPlanNodeId = "send_test_"+localPlanNode.getId();
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, edge_send_fragmentId, "0");
    int channelNum = 1;
    long query_num=1;
    FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
    DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
    sinkHandle =
            MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
                    Collections.singletonList(
                            new DownStreamChannelLocation(
                                    remoteEndpoint,
                                    remoteFragmentInstanceId,
                                    remotePlanNodeId)),
                    downStreamChannelIndex,
                    ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                    localFragmentInstanceId,
                    localPlanNodeId,
                    instanceContext);
    PipeInfo.getInstance().getJoinStatus(Integer.parseInt(localPlanNode.getId())).setSinkHandle(this.sinkHandle);
    if(PipeInfo.getInstance().getPipeStatus()){
      sinkHandle.tryOpenChannel(0);//打开通道
    }

  }

  @Override
  public TsBlock next() throws Exception {
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    if (!prepareInput(start, maxRuntime)) {
      return null;
    }
    PipeInfo pipeInfo=PipeInfo.getInstance();
    System.out.println("----status:"+pipeInfo.getPipeStatus());
    if (CloudEdgeCollaborativeFlag.getInstance().cloudEdgeCollaborativeFlag) {
//      if(pipeInfo.getPipeStatus()){
      // 构建pipeline的sourceHandle和sinkHandle
      createSinkHandle();
      createSourceHandle();

      // 布隆过滤器处理构造新块
      TsBlock resultBlock = null;
      TsBlock bloomFilterReference = null;
      int referenceIndex = 0;
      int smallestSize = Integer.MAX_VALUE;
      for (int i = 0; i < inputOperatorsCount; i++) {
        if (inputTsBlocks[i] != null && inputTsBlocks[i].getPositionCount() < smallestSize) {
          smallestSize = inputTsBlocks[i].getPositionCount();
          referenceIndex = i;
          bloomFilterReference = inputTsBlocks[i];
        }
      }
      if (bloomFilterReference == null) {
        return null;
      }
      BloomFilter filter =
          BloomFilter.getEmptyBloomFilter(
              TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), smallestSize);
      for (int i = 0; i < smallestSize; i++) {
        long time = bloomFilterReference.getTimeByIndex(i);
        filter.add(String.valueOf(time));
      }
      ArrayList<TsBlock> inputBlocksAfterProcess = new ArrayList<>();
      for (int i = 0; i < inputOperatorsCount; i++) {
        TsBlock block = inputTsBlocks[i];
        if (i == referenceIndex) {
          inputBlocksAfterProcess.add(block);
        } else {
          //          children.get(i).getResultBuilder();

          List<TSDataType> dataTypes = new ArrayList<>();
          for (int columnNum = 0; columnNum < block.getValueColumnCount(); columnNum++) {
            Column[] valueColumns = block.getValueColumns();
            switch (valueColumns[i].getDataType()) {
              case BOOLEAN:
                dataTypes.add(TSDataType.BOOLEAN);
                break;
              case INT32:
                dataTypes.add(TSDataType.INT32);
                break;
              case INT64:
                dataTypes.add(TSDataType.INT64);
                break;
              case FLOAT:
                dataTypes.add(TSDataType.FLOAT);
                break;
              case DOUBLE:
                dataTypes.add(TSDataType.DOUBLE);
                break;
              case TEXT:
                dataTypes.add(TSDataType.TEXT);
                break;
              default:
                throw new UnSupportedDataTypeException(
                    "Unknown datatype: " + valueColumns[i].getDataType());
            }
          }
          TsBlockBuilder resultBuilder = new TsBlockBuilder(dataTypes);

          // 用bloomfilter检验并构造新block
          TimeColumnBuilder timeColumnBuilder = resultBuilder.getTimeColumnBuilder();
          ArrayList<Integer> timeIndexArray = new ArrayList<>();
          for (int pos = 0; pos < block.getPositionCount(); pos++) {
            long time = block.getTimeByIndex(pos);
            if (filter.contains(String.valueOf(time))) {
              timeColumnBuilder.writeLong(time);
              resultBuilder.declarePosition();
              timeIndexArray.add(pos);
            }
          }
          buildValueColumns(block, timeIndexArray);

          resultBlock = resultBuilder.build();
          resultBuilder.reset();
          inputBlocksAfterProcess.add(resultBlock);
        }
      }
      // TODO: 测试一下构造的新block是否正确（所有能join上的都要在block内，join不上的无所谓）。可以考虑构造一些时间序列数据（尽量越多越好）在这输出一下看看。
      // 发送到云端
      if(!sinkHandle.isAborted()){
        sinkHandle.send(resultBlock);//发送数据
      }
      while(sinkHandle.getChannel(0).getNumOfBufferedTsBlocks()!=0){//防止有数据没有发送完就关闭通道
        try {
          Thread.sleep(10);//时间
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      sinkHandle.setNoMoreTsBlocksOfOneChannel(0);//关闭
      sinkHandle.close();
//      if(rev_tsblock==null& !Isrev){
//        Isrev=true;
//        // TODO: 如何发送数据到云端
//        ReceiveTsBlock receive=new ReceiveTsBlock();
//        rev_tsblock= receive.receive();
//        finish_rev=true;
//      }
//      while(!finish_rev){
//        Thread.sleep(10);
//      }


//      if(seriesScanUtil.dataType== TSDataType.BOOLEAN){
//        appendToBuilder(rev_tsblock);
//        flag_boolean=true;
//        flag_boolean=true;
//      }else{
//        appendToBuilder1(rev_tsblock);
//        flag_binary=true;
//        finish_binary=true;
//      }
//
//      if(flag_boolean==true&flag_binary==true){
//        rev_tsblock=null;
//        Isrev=false;
//        finish_rev=false;
//        finish_binary=false;
//        flag_boolean=false;
//      }
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
    resultBuilder.reset();
    return res;
  }

  /**
   * for rebuild tsblock after filter
   *
   * @param block
   * @param timeIndexArray
   */
  private void buildValueColumns(TsBlock block, ArrayList<Integer> timeIndexArray) {
    for (int i = 0; i < block.getValueColumnCount(); i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      Column column = block.getColumn(i);
      if (column.mayHaveNull()) {
        for (int rowIndex : timeIndexArray) {
          if (column.isNull(rowIndex)) {
            columnBuilder.appendNull();
          } else {
            columnBuilder.write(column, rowIndex);
          }
        }
      } else {
        for (int rowIndex : timeIndexArray) {
          columnBuilder.write(column, rowIndex);
        }
      }
    }
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
