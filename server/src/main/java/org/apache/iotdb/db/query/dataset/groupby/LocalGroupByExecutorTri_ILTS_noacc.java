/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
// package org.apache.iotdb.db.query.dataset.groupby;
//
// import org.apache.iotdb.db.conf.IoTDBConfig;
// import org.apache.iotdb.db.conf.IoTDBDescriptor;
// import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
// import org.apache.iotdb.db.exception.StorageEngineException;
// import org.apache.iotdb.db.exception.query.QueryProcessException;
// import org.apache.iotdb.db.metadata.PartialPath;
// import org.apache.iotdb.db.query.aggregation.AggregateResult;
// import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrResult;
// import org.apache.iotdb.db.query.context.QueryContext;
// import org.apache.iotdb.db.query.control.QueryResourceManager;
// import org.apache.iotdb.db.query.filter.TsFileFilter;
// import org.apache.iotdb.db.query.reader.series.SeriesReader;
// import org.apache.iotdb.db.utils.FileLoaderUtils;
// import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
// import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
// import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
// import org.apache.iotdb.tsfile.read.common.ChunkSuit4Tri;
// import org.apache.iotdb.tsfile.read.common.IOMonitor2;
// import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
// import org.apache.iotdb.tsfile.read.filter.basic.Filter;
// import org.apache.iotdb.tsfile.read.reader.page.PageReader;
// import org.apache.iotdb.tsfile.utils.Pair;
//
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.IOException;
// import java.nio.ByteBuffer;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Set;
//
// public class LocalGroupByExecutorTri_ILTS_noacc implements GroupByExecutor {
//
//  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
//  private static final Logger M4_CHUNK_METADATA = LoggerFactory.getLogger("M4_CHUNK_METADATA");
//
//  // Aggregate result buffer of this path
//  private final List<AggregateResult> results = new ArrayList<>();
//
//  // keys: 0,1,...,(int) Math.floor((endTime * 1.0 - startTime) / interval)-1
//  private final Map<Integer, List<ChunkSuit4Tri>> splitChunkList = new HashMap<>();
//
//  private final long p1t = CONFIG.getP1t();
//  private final double p1v = CONFIG.getP1v();
//  private final long pnt = CONFIG.getPnt();
//  private final double pnv = CONFIG.getPnv();
//
//  private long lt = p1t;
//  private double lv = p1v;
//
//  private final int N1; // 分桶数
//
//  private static final int numIterations = CONFIG.getNumIterations();
//
//  private Filter timeFilter;
//
//  public LocalGroupByExecutorTri_ILTS_noacc(
//      PartialPath path,
//      Set<String> allSensors,
//      TSDataType dataType,
//      QueryContext context,
//      Filter timeFilter,
//      TsFileFilter fileFilter,
//      boolean ascending)
//      throws StorageEngineException, QueryProcessException {
//    //    long start = System.nanoTime();
//
//    // get all data sources
//    QueryDataSource queryDataSource =
//        QueryResourceManager.getInstance().getQueryDataSource(path, context, this.timeFilter);
//
//    // update filter by TTL
//    this.timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);
//
//    SeriesReader seriesReader =
//        new SeriesReader(
//            path,
//            allSensors,
//            // fix bug: here use the aggregation type as the series data type,
//            // not using pageReader.getAllSatisfiedPageData is ok
//            dataType,
//            context,
//            queryDataSource,
//            timeFilter,
//            null,
//            fileFilter,
//            ascending);
//
//    // unpackAllOverlappedFilesToTimeSeriesMetadata
//    try {
//      // : this might be bad to load all chunk metadata at first
//      List<ChunkSuit4Tri> futureChunkList =
//          new ArrayList<>(seriesReader.getAllChunkMetadatas4Tri()); // no need sort here
//      // arrange futureChunkList into each bucket
//      GroupByFilter groupByFilter = (GroupByFilter) timeFilter;
//      long startTime = groupByFilter.getStartTime();
//      long endTime = groupByFilter.getEndTime();
//      long interval = groupByFilter.getInterval();
//      N1 = (int) Math.floor((endTime * 1.0 - startTime) / interval); // 分桶数
//      for (ChunkSuit4Tri chunkSuit4Tri : futureChunkList) {
//        ChunkMetadata chunkMetadata = chunkSuit4Tri.chunkMetadata;
//        long chunkMinTime = chunkMetadata.getStartTime();
//        long chunkMaxTime = chunkMetadata.getEndTime();
//        int idx1 = (int) Math.floor((chunkMinTime - startTime) * 1.0 / interval);
//        int idx2 = (int) Math.floor((chunkMaxTime - startTime) * 1.0 / interval);
//        idx2 = (int) Math.min(idx2, N1 - 1);
//        for (int i = idx1; i <= idx2; i++) {
//          splitChunkList.computeIfAbsent(i, k -> new ArrayList<>());
//          splitChunkList.get(i).add(chunkSuit4Tri);
//        }
//      }
//
//    } catch (IOException e) {
//      throw new QueryProcessException(e.getMessage());
//    }
//
//    //    IOMonitor2.addMeasure(Operation.M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS, System.nanoTime() -
//    // start);
//  }
//
//  @Override
//  public void addAggregateResult(AggregateResult aggrResult) {
//    results.add(aggrResult);
//  }
//
//  @Override
//  public List<AggregateResult> calcResult(
//      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
//      throws IOException {
//    // 这里用calcResult一次返回所有buckets结果（可以把MinValueAggrResult的value设为string类型，
//    // 那就把所有buckets结果作为一个string返回。这样的话返回的[t]是没有意义的，只取valueString）
//    // 而不是像MinMax那样在nextWithoutConstraintTri_MinMax()里调用calcResult每次计算一个bucket
//    StringBuilder series_final = new StringBuilder();
//
//    // clear result cache
//    for (AggregateResult result : results) {
//      result.reset();
//    }
//
//    long[] lastIter_t = new long[N1]; // N1不包括全局首尾点
//    double[] lastIter_v = new double[N1]; // N1不包括全局首尾点
//    for (int num = 0; num < numIterations; num++) {
//      //      StringBuilder series = new StringBuilder();
//      // 全局首点
//      //      series.append(p1v).append("[").append(p1t).append("]").append(",");
//      // 遍历分桶 Assume no empty buckets
//      for (int b = 0; b < N1; b++) {
//        long rt = 0; // must initialize as zero, because may be used as sum for average
//        double rv = 0; // must initialize as zero, because may be used as sum for average
//        // 计算右边桶的固定点
//        if (b == N1 - 1) { // 最后一个桶
//          // 全局尾点
//          rt = pnt;
//          rv = pnv;
//        } else { // 不是最后一个桶
//          if (num == 0) { // 是第一次迭代的话，就使用右边桶的平均点
//            // ========计算右边桶的平均点========
//            List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(b + 1);
//            long rightStartTime = startTime + (b + 1) * interval;
//            long rightEndTime = startTime + (b + 2) * interval;
//            int cnt = 0;
//            // 遍历所有与右边桶overlap的chunks
//            for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
//              TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
//              if (dataType != TSDataType.DOUBLE) {
//                throw new UnSupportedDataTypeException(String.valueOf(dataType));
//              }
//              // 1. load page data if it hasn't been loaded
//              if (chunkSuit4Tri.pageReader == null) {
//                chunkSuit4Tri.pageReader =
//                    FileLoaderUtils.loadPageReaderList4CPV(
//                        chunkSuit4Tri.chunkMetadata, this.timeFilter);
//                //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
//                //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
//                //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
//                //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
//                //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
//              }
//              // 2. 计算平均点
//              PageReader pageReader = chunkSuit4Tri.pageReader;
//              for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
//                long timestamp = pageReader.timeBuffer.getLong(j * 8);
//                if (timestamp < rightStartTime) {
//                  continue;
//                } else if (timestamp >= rightEndTime) {
//                  break;
//                } else { // rightStartTime<=t<rightEndTime
//                  ByteBuffer valueBuffer = pageReader.valueBuffer;
//                  double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
//                  rt += timestamp;
//                  rv += v;
//                  cnt++;
//                }
//              }
//            }
//            if (cnt == 0) {
//              throw new IOException("Empty bucket!");
//            }
//            rt = rt / cnt;
//            rv = rv / cnt;
//          } else { // 不是第一次迭代也不是最后一个桶的话，就使用上一轮迭代右边桶的采样点
//            rt = lastIter_t[b + 1];
//            rv = lastIter_v[b + 1];
//          }
//        }
//        // ========找到当前桶内距离lr连线最远的点========
//        double maxArea = -1;
//        long select_t = -1;
//        double select_v = -1;
//        List<ChunkSuit4Tri> chunkSuit4TriList = splitChunkList.get(b);
//        long localCurStartTime = startTime + (b) * interval;
//        long localCurEndTime = startTime + (b + 1) * interval;
//        // 遍历所有与当前桶overlap的chunks
//        for (ChunkSuit4Tri chunkSuit4Tri : chunkSuit4TriList) {
//          TSDataType dataType = chunkSuit4Tri.chunkMetadata.getDataType();
//          if (dataType != TSDataType.DOUBLE) {
//            throw new UnSupportedDataTypeException(String.valueOf(dataType));
//          }
//          // load page data if it hasn't been loaded
//          if (chunkSuit4Tri.pageReader == null) {
//            chunkSuit4Tri.pageReader =
//                FileLoaderUtils.loadPageReaderList4CPV(
//                    chunkSuit4Tri.chunkMetadata, this.timeFilter);
//            //  ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
//            //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
//            //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
//            //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS
//            //  ASSIGN DIRECTLY), WHICH WILL INTRODUCE BUGS!
//          }
//          PageReader pageReader = chunkSuit4Tri.pageReader;
//          for (int j = 0; j < chunkSuit4Tri.chunkMetadata.getStatistics().getCount(); j++) {
//            long timestamp = pageReader.timeBuffer.getLong(j * 8);
//            if (timestamp < localCurStartTime) {
//              continue;
//            } else if (timestamp >= localCurEndTime) {
//              break;
//            } else { // localCurStartTime<=t<localCurEndTime
//              ByteBuffer valueBuffer = pageReader.valueBuffer;
//              double v = valueBuffer.getDouble(pageReader.timeBufferLength + j * 8);
//              double area = IOMonitor2.calculateTri(lt, lv, timestamp, v, rt, rv);
//              if (area > maxArea) {
//                maxArea = area;
//                select_t = timestamp;
//                select_v = v;
//              }
//            }
//          }
//        }
//        // 记录结果
//        //        series.append(select_v).append("[").append(select_t).append("]").append(",");
//
//        // 更新lt,lv
//        // 下一个桶自然地以select_t, select_v作为左桶固定点
//        lt = select_t;
//        lv = select_v;
//        // 记录本轮迭代本桶选点
//        lastIter_t[b] = select_t;
//        lastIter_v[b] = select_v;
//      } // 遍历分桶结束
//
//      // 全局尾点
//      //      series.append(pnv).append("[").append(pnt).append("]").append(",");
//      //      System.out.println(series);
//
//    } // end Iterations
//
//    // 全局首点
//    series_final.append(p1v).append("[").append(p1t).append("]").append(",");
//    for (int i = 0; i < lastIter_t.length; i++) {
//
// series_final.append(lastIter_v[i]).append("[").append(lastIter_t[i]).append("]").append(",");
//    }
//    // 全局尾点
//    series_final.append(pnv).append("[").append(pnt).append("]").append(",");
//    MinValueAggrResult minValueAggrResult = (MinValueAggrResult) results.get(0);
//    minValueAggrResult.updateResult(new MinMaxInfo<>(series_final.toString(), 0));
//
//    return results;
//  }
//
//  @Override
//  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
//      throws IOException {
//    throw new IOException("no implemented");
//  }
//
//  @Override
//  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
//      throws IOException, QueryProcessException {
//    throw new IOException("no implemented");
//  }
// }