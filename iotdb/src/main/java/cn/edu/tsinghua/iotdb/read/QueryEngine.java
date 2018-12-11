package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBSeriesReader;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.util.ExpressionOptimizer;
import cn.edu.tsinghua.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

public class QueryEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryEngine.class);

  public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
    if (queryExpression.hasQueryFilter()) {
      try {

        IExpression optimizedExpression = ExpressionOptimizer.getInstance().
                optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);

        if (optimizedExpression.getType() == GLOBAL_TIME) {
          return QueryWithGlobalTimeFilterExecutorImpl.execute(queryExpression);
        } else {
          return QueryWithFilterExecutorImpl.execute(queryExpression);
        }

      } catch (QueryFilterOptimizationException e) {
        throw new IOException(e);
      }
    } else {
      return execute(queryExpression);
    }
  }

  public QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

    List<FileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : queryExpression.getSelectedSeries()) {
      QueryDataSource queryDataSource = QueryDataSourceExecutor.getQueryDataSource(path);
      FileSeriesReader seriesReader = new IoTDBSeriesReader(queryDataSource);
    }

    return new DataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes, readersOfSelectedSeries);
  }

}
