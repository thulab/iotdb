package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.QueryJobManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithFilter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithoutFilter;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p> A reader for sequentially inserts data，including a list of sealedTsFile, unSealedTsFile
 * and data in MemTable.
 */
public abstract class SequenceDataReader implements ISeriesReader {

  protected List<ISeriesReader> seriesReaders;
  protected long jobId;

  private boolean hasSeriesReaderInitialized;
  private int nextSeriesReaderIndex;
  private ISeriesReader currentSeriesReader;

  public SequenceDataReader(GlobalSortedSeriesDataSource sortedSeriesDataSource, SingleSeriesExpression singleSeriesExpression) throws IOException {
    seriesReaders = new ArrayList<>();
    jobId = QueryJobManager.getInstance().addJobForOneQuery();

    hasSeriesReaderInitialized = false;
    nextSeriesReaderIndex = 0;

    // add data in sealedTsFiles and unSealedTsFile
    if (sortedSeriesDataSource.getSealedTsFiles() != null) {
      seriesReaders.add(new SealedTsFileReader(sortedSeriesDataSource, singleSeriesExpression));
    }
    if (sortedSeriesDataSource.getUnsealedTsFile() != null) {
      seriesReaders.add(new UnSealedTsFileReader(sortedSeriesDataSource.getUnsealedTsFile(), singleSeriesExpression));
    }

    // add data in memTable
    if (sortedSeriesDataSource.hasRawSeriesChunk() && singleSeriesExpression == null) {
      seriesReaders.add(new RawSeriesChunkReaderWithoutFilter(sortedSeriesDataSource.getRawSeriesChunk()));
    }
    if (sortedSeriesDataSource.hasRawSeriesChunk() && singleSeriesExpression != null) {
      seriesReaders.add(new RawSeriesChunkReaderWithFilter(sortedSeriesDataSource.getRawSeriesChunk(), singleSeriesExpression));
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasSeriesReaderInitialized && currentSeriesReader.hasNext()) {
      return true;
    } else {
      hasSeriesReaderInitialized = false;
    }

    while (nextSeriesReaderIndex < seriesReaders.size()) {
      if (!hasSeriesReaderInitialized) {
        currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
        hasSeriesReaderInitialized = true;
      }
      if (currentSeriesReader.hasNext()) {
        return true;
      } else {
        hasSeriesReaderInitialized = false;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    return currentSeriesReader.next();
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    next();
  }

  @Override
  public void close() throws IOException {
    for (ISeriesReader seriesReader : seriesReaders) {
      seriesReader.close();
    }
  }

}
