package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.merge.Priority;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.merge.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.merge.PrioritySeriesReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.sequence.SequenceDataReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.unsequence.UnSeqSeriesReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.read.dataset.DataSetWithoutTimeGenerator;
//import cn.edu.tsinghua.iotdb.read.executor.QueryWithFilterExecutorImpl;
//import cn.edu.tsinghua.iotdb.read.executor.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.util.ExpressionOptimizer;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

public class EngineQueryExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(EngineQueryExecutor.class);

  public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
    if (queryExpression.hasQueryFilter()) {
      try {

        IExpression optimizedExpression = ExpressionOptimizer.getInstance().
                optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);

        if (optimizedExpression.getType() == GLOBAL_TIME) {
//          return QueryWithGlobalTimeFilterExecutorImpl.execute(queryExpression);
        } else {
//          return QueryWithFilterExecutorImpl.execute(queryExpression);
        }

      } catch (QueryFilterOptimizationException e) {
        throw new IOException(e);
      }
    } else {
      return execute(queryExpression);
    }
    
    return null;
  }

  public QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

    List<IReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : queryExpression.getSelectedSeries()) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(path);
      PriorityMergeReader priorityReader = new PriorityMergeReader();

      // sequence insert data
      SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeriesDataSource(), null);
      priorityReader.addReaderWithPriority(tsFilesReader, 1);

      // unseq insert data
      UnSeqSeriesReader unSeqSeriesReader = SeriesReaderFactory.getInstance().
              createSeriesReaderForUnSeq(queryDataSource.getOverflowSeriesDataSource());
      priorityReader.addReaderWithPriority(unSeqSeriesReader, 2);

      readersOfSelectedSeries.add(priorityReader);
    }

    return new DataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes, readersOfSelectedSeries);
  }

}
