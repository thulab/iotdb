package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.UnSeqSeriesReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

import java.io.IOException;

/**
 * <p> A single IoTDB series data reader with seriesFilter, which has considered sequence insert data, unseq insert data,
 * update and delete operation.
 */
public class EngineSeriesReader implements ISeriesReader {

  // merge reader

  // without SingleSeriesExpression
  public EngineSeriesReader(QueryDataSource queryDataSource) throws IOException {

    // sequence insert data
    SeqSeriesReader tsFilesReader = new SeqSeriesReader(queryDataSource.getSeriesDataSource(), null);
    PriorityTimeValuePairReader tsFilesReaderWithPriority = new PriorityTimeValuePairReader(
            tsFilesReader, new PriorityTimeValuePairReader.Priority(1));

    // unseq insert data
    UnSeqSeriesReader unSeqSeriesReader = SeriesReaderFactory.getInstance().
            createSeriesReaderForUnSeq(queryDataSource.getOverflowSeriesDataSource());
    PriorityTimeValuePairReader overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReader(
            unSeqSeriesReader, new PriorityTimeValuePairReader.Priority(2));


    PriorityMergeReader insertDataReader = new PriorityMergeReader(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
  }

  // with SingleSeriesExpression
  public EngineSeriesReader(QueryDataSource queryDataSource, SingleSeriesExpression singleSeriesExpression) throws IOException {

    int priority = 1;

    // sequence insert data
    SeqSeriesReader tsFilesReader =
            new SeqSeriesReader(queryDataSource.getSeriesDataSource(), singleSeriesExpression);
    PriorityTimeValuePairReader tsFilesReaderWithPriority = new PriorityTimeValuePairReader(
            tsFilesReader, new PriorityTimeValuePairReader.Priority(priority++));

    // unseq insert data
    UnSeqSeriesReader unSeqSeriesReader = SeriesReaderFactory.getInstance().
            createSeriesReaderForUnSeq(queryDataSource.getOverflowSeriesDataSource(), singleSeriesExpression.getFilter());
    PriorityTimeValuePairReader overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReader(
            unSeqSeriesReader, new PriorityTimeValuePairReader.Priority(priority++));


    PriorityMergeReader insertDataReader =
            new PriorityMergeReader(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
  }

  @Override
  public boolean hasNext() throws IOException {

  }

  @Override
  public TimeValuePair next() throws IOException {

  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNextBatch() {
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return null;
  }

  @Override
  public BatchData currentBatch() {
    return null;
  }
}
