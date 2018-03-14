package cn.edu.tsinghua.iotdb.query.engine.segmentby;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.aggregationv2.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.v2.ReaderType;
import cn.edu.tsinghua.iotdb.query.v2.QueryRecordReader;
import cn.edu.tsinghua.iotdb.query.v2.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.udf.AbstractUDSF;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.SegmentQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.aggregationKey;

/**
 * Segment by aggregation implementation with <code>FilterStructure</code>.
 */
public class SegmentByEngineWithFilter {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentByEngineWithFilter.class);

  /**
   * aggregateFetchSize is set to calculate the result of timestamps, when the size of common
   * timestamps is up to aggregateFetchSize, the aggregation calculation process will begin
   **/
  private int aggregateFetchSize = TsfileDBDescriptor.getInstance().getConfig().fetchSize;

  /**
   * crossQueryFetchSize is sed to read one column data, this variable is mainly used to debug to
   * verify the rightness of iterative readOneColumnWithoutFilter
   **/
  private int crossQueryFetchSize = TsfileDBDescriptor.getInstance().getConfig().fetchSize;

  /**
   * all the segment by Path ans its AggregateFunction
   **/
  private List<Pair<Path, AggregateFunction>> aggregations;

  private AbstractUDSF udsf;

  /**
   * filter in segment by where clause
   **/
  private List<FilterStructure> filterStructures;

  /**
   * segment by partition fetch size, when result size is reach to partitionSize, the current
   * calculation will be terminated
   */
  private int partitionFetchSize;

  /**
   * HashMap to record the query result of each aggregation Path
   **/
  private Map<String, DynamicOneColumnData> queryPathResult = new HashMap<>();

  /**
   * represent duplicated path index
   **/
  private Set<Integer> duplicatedPaths = new HashSet<>();

  /**
   * segment by result
   **/
  private QueryDataSet segmentByResult = new SegmentQueryDataSet();

  // variables below are used to calculate the common timestamps of FilterStructures

  /**
   * stores the query QueryDataSet of each FilterStructure in filterStructures
   **/
  private List<QueryDataSet> fsDataSets = new ArrayList<>();

  /**
   * stores calculated common timestamps of each FilterStructure
   **/
  private List<long[]> fsTimeList = new ArrayList<>();

  /**
   * stores used index of each fsTimeList
   **/
  private List<Integer> fsTimeIndexList = new ArrayList<>();

  /**
   * represents whether this FilterStructure answer still has unread data
   **/
  private List<Boolean> fsHasUnReadDataList = new ArrayList<>();

  /**
   * the aggregate timestamps calculated by all FilterStructures
   **/
  private List<Long> aggregateTimestamps = new ArrayList<>();

  /**
   * priority queue to store timestamps of each FilterStructure
   **/
  private PriorityQueue<Long> commonTimeQueue = new PriorityQueue<>();

  private boolean queryCalcFlag = true;

  public SegmentByEngineWithFilter(List<Pair<Path, AggregateFunction>> aggregations,
      List<FilterStructure> filterStructures,
      AbstractUDSF udsf, int partitionFetchSize)
      throws IOException, ProcessorException {
    this.aggregations = aggregations;
    this.filterStructures = filterStructures;
    this.udsf = udsf;
    this.partitionFetchSize = partitionFetchSize;

    this.queryPathResult = new HashMap<>();
    for (int i = 0; i < aggregations.size(); i++) {
      String aggregateKey = aggregationKey(aggregations.get(i).right, aggregations.get(i).left);
      if (!segmentByResult.mapRet.containsKey(aggregateKey)) {
        segmentByResult.mapRet.put(aggregateKey,
            new DynamicOneColumnData(aggregations.get(i).right.dataType, true, true));
        queryPathResult.put(aggregateKey, null);
      } else {
        duplicatedPaths.add(i);
      }
    }

    for (int idx = 0; idx < filterStructures.size(); idx++) {
      FilterStructure filterStructure = filterStructures.get(idx);
      QueryDataSet queryDataSet = new QueryDataSet();
      queryDataSet.crossQueryTimeGenerator = new CrossQueryTimeGenerator(
          filterStructure.getTimeFilter(),
          filterStructure.getFrequencyFilter(), filterStructure.getValueFilter(),
          crossQueryFetchSize) {
        @Override
        public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
            SingleSeriesFilterExpression valueFilter, int valueFilterNumber)
            throws ProcessorException, IOException {
          try {
            return getDataUseSingleValueFilter(valueFilter, res, fetchSize, valueFilterNumber);
          } catch (PathErrorException e) {
            e.printStackTrace();
            return null;
          }
        }
      };
      fsDataSets.add(queryDataSet);
      long[] commonTimestamps = queryDataSet.crossQueryTimeGenerator.generateTimes();
      fsTimeList.add(commonTimestamps);
      fsTimeIndexList.add(0);
      if (commonTimestamps.length > 0) {
        fsHasUnReadDataList.add(true);
      } else {
        fsHasUnReadDataList.add(false);
      }
    }

    LOG.debug("construct SegmentByEngineWithFilter successfully");
  }

  /**
   * @return
   * @throws IOException
   * @throws ProcessorException
   */
  public QueryDataSet segmentBy() throws IOException, ProcessorException, PathErrorException {

    segmentByResult.clear();
    int segmentCount = 0;

    if (commonTimeQueue.isEmpty()) {
      for (int i = 0; i < fsTimeList.size(); i++) {
        boolean flag = fsHasUnReadDataList.get(i);
        if (flag) {
          commonTimeQueue.add(fsTimeList.get(i)[fsTimeIndexList.get(i)]);
        }
      }
    }

    int aggregationOrdinal = 0;
    for (Pair<Path, AggregateFunction> pair : aggregations) {
      if (duplicatedPaths.contains(aggregationOrdinal)) {
        continue;
      }
      aggregationOrdinal++;

      int segmentIdx = 0;
      calcAggregateTimestamps();

      Path path = pair.left;
      AggregateFunction aggregateFunction = pair.right;
      String aggregationKey = aggregationKey(aggregateFunction, path);
      DynamicOneColumnData data = queryPathResult.get(aggregationKey);

      if (data.timeLength == 0) {
        continue;
      } else {
        udsf.setLastTime(data.getTime(data.curIdx));
        udsf.setLastValue(data.getAnObject(data.curIdx));
      }

      while (true) {
        aggregateFunction.calcSegmentByAggregation(segmentIdx, udsf, data);
        if (data.curIdx >= data.timeLength) {
          aggregateTimestamps.clear();
          queryCalcFlag = true;
          calcAggregateTimestamps();
          data = queryPathResult.get(aggregationKey);
        }
        if (data.timeLength == 0) {
          aggregateFunction.resultData.putEmptyTime(udsf.getLastTime());
          segmentCount++;
          break;
        }

        if (aggregateFunction.resultData.emptyTimeLength - 1 == segmentIdx) {
          segmentIdx++;
          segmentCount++;
        }
        if (segmentCount >= partitionFetchSize) {
          break;
        }
      }

      if (segmentCount >= partitionFetchSize) {
        break;
      }
    }

    int cnt = 0;
    for (Pair<Path, AggregateFunction> pair : aggregations) {
      if (duplicatedPaths.contains(cnt)) {
        continue;
      }
      cnt++;
      Path path = pair.left;
      AggregateFunction aggregateFunction = pair.right;
      segmentByResult.mapRet.put(aggregationKey(aggregateFunction, path), aggregateFunction.resultData);
    }

    return segmentByResult;
  }

  private void calcAggregateTimestamps()
      throws ProcessorException, PathErrorException, IOException {
    while (aggregateTimestamps.size() < aggregateFetchSize && !commonTimeQueue.isEmpty()) {
      // add the minimum timestamp in commonTimeQueue,
      // remove other time which is equals to minimum timestamp in fsTimeList
      long minTime = commonTimeQueue.poll();
      aggregateTimestamps.add(minTime);
      while (!commonTimeQueue.isEmpty() && minTime == commonTimeQueue.peek()) {
        commonTimeQueue.poll();
      }

      for (int i = 0; i < fsTimeList.size(); i++) {
        boolean flag = fsHasUnReadDataList.get(i);
        if (flag) {
          int curTimeIdx = fsTimeIndexList.get(i);
          long[] curTimestamps = fsTimeList.get(i);
          // remove all timestamps equal to min time in all series
          while (curTimeIdx < curTimestamps.length && curTimestamps[curTimeIdx] == minTime) {
            curTimeIdx++;
          }
          if (curTimeIdx < curTimestamps.length) {
            fsTimeIndexList.set(i, curTimeIdx);
            commonTimeQueue.add(curTimestamps[curTimeIdx]);
          } else {
            long[] newTimeStamps = fsDataSets.get(i).crossQueryTimeGenerator.generateTimes();
            if (newTimeStamps.length > 0) {
              fsTimeList.set(i, newTimeStamps);
              fsTimeIndexList.set(i, 0);
            } else {
              fsHasUnReadDataList.set(i, false);
            }
          }
        }
      }
    }

    if (queryCalcFlag) {
      calcPathQueryData();
      queryCalcFlag = false;
    }
  }

  private void calcPathQueryData() throws ProcessorException, PathErrorException, IOException {
    int aggregationOrdinal = 0;
    for (Pair<Path, AggregateFunction> pair : aggregations) {
      Path path = pair.left;
      AggregateFunction aggregateFunction = pair.right;
      String aggregationKey = aggregationKey(aggregateFunction, path);
      if (duplicatedPaths.contains(aggregationOrdinal)) {
        continue;
      }
      aggregationOrdinal++;

      DynamicOneColumnData data = queryPathResult.get(aggregationKey);
      // common aggregate timestamps is empty
      // the query data of path should be clear too
      if (aggregateTimestamps.size() == 0) {
        if (data != null) {
          data.clearData();
        } else {
          data = new DynamicOneColumnData(aggregateFunction.dataType, true);
          queryPathResult.put(aggregationKey, data);
        }
        queryCalcFlag = true;
        continue;
      }

      String deltaObjectId = path.getDeltaObjectToString();
      String measurementId = path.getMeasurementToString();
      String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(aggregationOrdinal);
      QueryRecordReader recordReader = (QueryRecordReader) RecordReaderFactory.getInstance()
          .getRecordReader(deltaObjectId, measurementId,
              null, null, null, recordReaderPrefix, ReaderType.QUERY);

      data = recordReader
          .queryUsingTimestamps(aggregateTimestamps.stream().mapToLong(i -> i).toArray());
      queryPathResult.put(aggregationKey, data);
    }
  }

  /**
   * This function is only used for CrossQueryTimeGenerator. A CrossSeriesFilterExpression is
   * consist of many SingleSeriesFilterExpression. e.g. CSAnd(d1.s1, d2.s1) is consist of d1.s1 and
   * d2.s1, so this method would be invoked twice, once for querying d1.s1, once for querying d2.s1.
   * <p> When this method is invoked, need add the filter index as a new parameter, for the reason
   * of exist of <code>RecordReaderCache</code>, if the composition of CrossFilterExpression exist
   * same SingleFilterExpression, we must guarantee that the <code>RecordReaderCache</code> doesn't
   * cause conflict to the same SingleFilterExpression.
   */
  private static DynamicOneColumnData getDataUseSingleValueFilter(
      SingleSeriesFilterExpression queryValueFilter,
      DynamicOneColumnData res, int fetchSize, int valueFilterNumber)
      throws ProcessorException, IOException, PathErrorException {

    String deltaObjectUID = ((SingleSeriesFilterExpression) queryValueFilter).getFilterSeries()
        .getDeltaObjectUID();
    String measurementUID = ((SingleSeriesFilterExpression) queryValueFilter).getFilterSeries()
        .getMeasurementUID();
    //TODO may have dnf conflict
    String valueFilterPrefix = ReadCachePrefix.addFilterPrefix(valueFilterNumber);

    QueryRecordReader recordReader = (QueryRecordReader) RecordReaderFactory.getInstance()
        .getRecordReader(deltaObjectUID, measurementUID,
            null, queryValueFilter, null, valueFilterPrefix, ReaderType.QUERY);

    if (res == null) {
      res = recordReader.queryOneSeries(null, queryValueFilter, null, fetchSize);
    } else {
      res = recordReader.queryOneSeries(null, queryValueFilter, res, fetchSize);
    }

    return res;
  }
}
