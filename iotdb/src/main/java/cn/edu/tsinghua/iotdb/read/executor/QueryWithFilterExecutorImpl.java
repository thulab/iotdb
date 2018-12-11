package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceManager;
import cn.edu.tsinghua.iotdb.read.reader.QueryByTimestampsReader;
import cn.edu.tsinghua.iotdb.read.timegenerator.EngineTimeGenerator;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor with filter
 */
public class QueryWithFilterExecutorImpl {

  public QueryWithFilterExecutorImpl() {
  }

  public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

    TimestampGenerator timestampGenerator = new EngineTimeGenerator(queryExpression.getExpression());

    LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries = new LinkedHashMap<>();
    initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), queryExpression.getExpression());
    return new DataSetWithQueryFilter(timestampGenerator, readersOfSelectedSeries);
  }

  private static void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries,
                                                  List<Path> selectedSeries, IExpression expression)
                                                  throws IOException, FileNodeManagerException {

    LinkedHashMap<Path, SeriesFilter> path2SeriesFilter = parseSeriesFilter(expression);

    for (Path path : selectedSeries) {
      QueryDataSource queryDataSource = null;
      if (path2SeriesFilter.containsKey(path)) {
        queryDataSource = QueryDataSourceManager.getQueryDataSource(path2SeriesFilter.get(path));
      } else {
        queryDataSource = QueryDataSourceManager.getQueryDataSource(path);
      }
      SeriesReaderByTimeStamp seriesReader = new QueryByTimestampsReader(queryDataSource);
      readersOfSelectedSeries.put(path, seriesReader);
    }
  }

  private static LinkedHashMap parseSeriesFilter(QueryFilter queryFilter) {
    LinkedHashMap<Path, SeriesFilter> path2SeriesFilter = new LinkedHashMap<Path, SeriesFilter>();
    traverse(queryFilter, path2SeriesFilter);
    return path2SeriesFilter;
  }

  private static void traverse(QueryFilter queryFilter, LinkedHashMap path2SeriesFilter) {
    if (queryFilter.getType() == QueryFilterType.SERIES) {
      SeriesFilter seriesFilter = (SeriesFilter) queryFilter;
      path2SeriesFilter.put(seriesFilter.getSeriesPath(), seriesFilter);
    } else {
      BinaryQueryFilter binaryQueryFilter = (BinaryQueryFilter) queryFilter;
      traverse(binaryQueryFilter.getLeft(), path2SeriesFilter);
      traverse(binaryQueryFilter.getRight(), path2SeriesFilter);
    }
  }
}
