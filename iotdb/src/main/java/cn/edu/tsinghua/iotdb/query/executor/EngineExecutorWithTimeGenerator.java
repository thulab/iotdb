package cn.edu.tsinghua.iotdb.query.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.dataset.EngineDataSetWithTimeGenerator;
import cn.edu.tsinghua.iotdb.query.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.merge.EngineReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.query.reader.merge.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.query.reader.merge.PriorityMergeReaderByTimestamp;
import cn.edu.tsinghua.iotdb.query.reader.sequence.SequenceDataReader;
import cn.edu.tsinghua.iotdb.query.control.QueryDataSourceManager;
import cn.edu.tsinghua.iotdb.query.timegenerator.EngineTimeGenerator;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IoTDB query executor with filter
 */
public class EngineExecutorWithTimeGenerator {


  public static QueryDataSet execute(long jobId, QueryExpression queryExpression) throws IOException, FileNodeManagerException {

    EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(queryExpression.getExpression());

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = getReadersOfSelectedPaths(jobId, queryExpression.getSelectedSeries());

    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : queryExpression.getSelectedSeries()) {
      try {
        dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
      } catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }

    }
    return new EngineDataSetWithTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
            timestampGenerator, readersOfSelectedSeries);
  }

  private static List<EngineReaderByTimeStamp> getReadersOfSelectedPaths(long jobId, List<Path> paths)
          throws IOException, FileNodeManagerException {

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(path);
      PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

      // reader for sequence data
      SequenceDataReader tsFilesReader = new SequenceDataReader(jobId, queryDataSource.getSeqDataSource(), null);
      mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

      // reader for unSequence data
      PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance().
              createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);
      mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

}
