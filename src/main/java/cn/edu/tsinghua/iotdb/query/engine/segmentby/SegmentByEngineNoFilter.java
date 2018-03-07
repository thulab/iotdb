package cn.edu.tsinghua.iotdb.query.engine.segmentby;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.aggregationv2.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.v2.QueryRecordReader;
import cn.edu.tsinghua.iotdb.query.v2.ReaderType;
import cn.edu.tsinghua.iotdb.query.v2.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.udf.AbstractUDSF;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.aggregationKey;

/**
 * Group by aggregation implementation without <code>FilterStructure</code>.
 */
public class SegmentByEngineNoFilter {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentByEngineNoFilter.class);

  /**
   * queryFetchSize is sed to read one column data, this variable is mainly used to debug to verify
   * the rightness of iterative readOneColumnWithoutFilter
   **/
  private int queryFetchSize = TsfileDBDescriptor.getInstance().getConfig().fetchSize;

  /**
   * all the group by Path ans its AggregateFunction
   **/
  private List<Pair<Path, AggregateFunction>> aggregations;

  private AbstractUDSF udsf;

  /**
   * group by partition fetch size, when result size is reach to partitionSize, the current
   * calculation will be terminated.
   * this variable could be set small to test
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

  private QueryDataSet groupByResult = new QueryDataSet();

  private SingleSeriesFilterExpression queryTimeFilter;

  public SegmentByEngineNoFilter(List<Pair<Path, AggregateFunction>> aggregations,
      SingleSeriesFilterExpression queryTimeFilter,
      AbstractUDSF udsf, int partitionFetchSize) {
    this.aggregations = aggregations;
    this.queryTimeFilter = queryTimeFilter;
    this.queryPathResult = new HashMap<>();
    for (int i = 0; i < aggregations.size(); i++) {
      String aggregateKey = aggregationKey(aggregations.get(i).right, aggregations.get(i).left);
      if (!groupByResult.mapRet.containsKey(aggregateKey)) {
        groupByResult.mapRet.put(aggregateKey,
            new DynamicOneColumnData(aggregations.get(i).right.dataType, true, true));
        queryPathResult.put(aggregateKey, null);
      } else {
        duplicatedPaths.add(i);
      }
    }

    this.udsf = udsf;
    this.partitionFetchSize = partitionFetchSize;
  }

  public QueryDataSet groupBy()
      throws IOException, ProcessorException, PathErrorException {

    groupByResult.clear();
    int segmentCount = 0;

    int aggregationOrdinal = 0;
    for (Pair<Path, AggregateFunction> pair : aggregations) {
      if (duplicatedPaths.contains(aggregationOrdinal)) {
        continue;
      }
      aggregationOrdinal++;

      int segmentIdx = 0;
      Path path = pair.left;
      AggregateFunction aggregateFunction = pair.right;
      String aggregationKey = aggregationKey(aggregateFunction, path);
      DynamicOneColumnData data = queryPathResult.get(aggregationKey);
      if (data == null || data.curIdx >= data.timeLength) {
        data = readOneColumnWithoutFilter(path, data, null, aggregationOrdinal);
        queryPathResult.put(aggregationKey, data);
      }

      if (data.timeLength == 0) {
        continue;
      }

      while (true) {
        aggregateFunction.calcSegmentByAggregation(segmentIdx, udsf, data);
        if (data.curIdx >= data.timeLength) {
          data = readOneColumnWithoutFilter(path, data, null, aggregationOrdinal);
          queryPathResult.put(aggregationKey, data);
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
      groupByResult.mapRet
          .put(aggregationKey(aggregateFunction, path), aggregateFunction.resultData);
    }

    return groupByResult;
  }

  private DynamicOneColumnData readOneColumnWithoutFilter(Path path, DynamicOneColumnData res,
      Integer readLock, int aggregationOrdinal)
      throws ProcessorException, IOException, PathErrorException {

    // this read process is batch read
    // every time the ```partitionFetchSize``` data size will be return

    String deltaObjectID = path.getDeltaObjectToString();
    String measurementID = path.getMeasurementToString();
    String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(aggregationOrdinal);

    QueryRecordReader recordReader = (QueryRecordReader)
        RecordReaderFactory.getInstance().getRecordReader(deltaObjectID, measurementID,
            queryTimeFilter, null, readLock, recordReaderPrefix, ReaderType.QUERY);

    if (res == null) {
      res = recordReader.queryOneSeries(queryTimeFilter, null, null, queryFetchSize);
    } else {
      res.clearData();
      res = recordReader.queryOneSeries(queryTimeFilter, null, res, queryFetchSize);
    }

    return res;
  }
}
