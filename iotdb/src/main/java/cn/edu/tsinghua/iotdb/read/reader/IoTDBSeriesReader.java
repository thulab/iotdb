package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.read.TimeValuePairReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

import java.io.IOException;

/**
 * <p> A single series data reader with seriesFilter, which has considered sequence insert data, overflow data,
 * update and delete operation.
 */
public class IoTDBSeriesReader implements TimeValuePairReader {

  public IoTDBSeriesReader(QueryDataSource queryDataSource, SingleSeriesExpression singleSeriesExpression) throws IOException {
    int priority = 1;
    // sequence insert data
    IoTDBSequenceDataReader tsFilesReader =
            new IoTDBSequenceDataReader(queryDataSource.getSeriesDataSource(), singleSeriesExpression);
    PriorityTimeValuePairReader tsFilesReaderWithPriority = new PriorityTimeValuePairReader(
            tsFilesReader, new PriorityTimeValuePairReader.Priority(priority++));

    // overflow insert data
    OverflowInsertDataReader overflowInsertDataReader = SeriesReaderFactory.getInstance().
            createSeriesReaderForOverflowInsert(queryDataSource.getOverflowSeriesDataSource(), singleSeriesExpression.getFilter());
    PriorityTimeValuePairReader overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReader(
            overflowInsertDataReader, new PriorityTimeValuePairReader.Priority(priority++));


    PriorityMergeSortTimeValuePairReader insertDataReader =
            new PriorityMergeSortTimeValuePairReader(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
  }

  public IoTDBSeriesReader(QueryDataSource queryDataSource) throws IOException {
    int priority = 1;
    // sequence insert data
    IoTDBSequenceDataReader tsFilesReader = new IoTDBSequenceDataReader(queryDataSource.getSeriesDataSource(), null);
    PriorityTimeValuePairReader tsFilesReaderWithPriority = new PriorityTimeValuePairReader(
            tsFilesReader, new PriorityTimeValuePairReader.Priority(priority++));

    // overflow insert data
    OverflowInsertDataReader overflowInsertDataReader = SeriesReaderFactory.getInstance().
            createSeriesReaderForOverflowInsert(queryDataSource.getOverflowSeriesDataSource());
    PriorityTimeValuePairReader overflowInsertDataReaderWithPriority = new PriorityTimeValuePairReader(
            overflowInsertDataReader, new PriorityTimeValuePairReader.Priority(priority++));


    PriorityMergeSortTimeValuePairReader insertDataReader =
            new PriorityMergeSortTimeValuePairReader(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
  }

  @Override
  public boolean hasNext() throws IOException {
    return seriesWithOverflowOpReader.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {
    return seriesWithOverflowOpReader.next();
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    seriesWithOverflowOpReader.skipCurrentTimeValuePair();
  }

  @Override
  public void close() throws IOException {
    seriesWithOverflowOpReader.close();
  }
}
