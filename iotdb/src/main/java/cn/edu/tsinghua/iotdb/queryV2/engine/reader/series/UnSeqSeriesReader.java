package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.control.OverflowFileStreamManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;

/**
 * A FileSeriesReader implement which used for read insert data of one series in Overflow.
 * This class contains a unique jobId which used identify different UnSeqSeriesReader.
 * <p>
 * IMPORTANT: Remember invoke {@code close()} to close the file stream(s) opened.
 * <p/>
 * <p>
 */
public class UnSeqSeriesReader implements ISeriesReader {

  private Long jobId;
  private PriorityMergeReader seriesReader;

  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  public UnSeqSeriesReader(Long jobId, PriorityMergeReader seriesReader) {
    this.jobId = jobId;
    this.seriesReader = seriesReader;
  }

  @Override
  public boolean hasNext() throws IOException {
    return seriesReader.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (hasCachedTimeValuePair) {
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    }
    return seriesReader.next();
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    seriesReader.skipCurrentTimeValuePair();
  }

  /**
   * Retrieves, but does not remove, next {@code TimeValuePair} in this FileSeriesReader,
   * or returns {@code null} if this there is no {@code TimeValuePair} in this FileSeriesReader.
   *
   * @return
   * @throws IOException
   */
  public TimeValuePair peek() throws IOException {
    if (hasCachedTimeValuePair || hasNext()) {
      if (hasCachedTimeValuePair) {
        return cachedTimeValuePair;
      } else {
        cachedTimeValuePair = next();
        hasCachedTimeValuePair = true;
        return cachedTimeValuePair;
      }
    } else {
      return null;
    }

  }

  /**
   * Close the file stream opened by current reader. Please invoke this method before release current reader.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    seriesReader.close();
    OverflowFileStreamManager.getInstance().closeAll(this.jobId);
  }

  public Long getJobId() {
    return this.jobId;
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
