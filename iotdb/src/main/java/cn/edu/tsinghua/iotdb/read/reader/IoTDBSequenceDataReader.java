package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.OverflowFileStreamManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithFilter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.read.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.read.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.read.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.read.filter.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.read.reader.page.SeriesReaderFromSingleFileWithFilterImpl;
import cn.edu.tsinghua.tsfile.read.reader.page.SeriesReaderFromSingleFileWithoutFilterImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class IoTDBSequenceDataReader extends SequenceDataReader {

  private SingleSeriesExpression singleSeriesExpression;

  public IoTDBSequenceDataReader(GlobalSortedSeriesDataSource sortedSeriesDataSource, SingleSeriesExpression singleSeriesExpression)
          throws IOException {

    super(sortedSeriesDataSource);

    this.singleSeriesExpression = singleSeriesExpression;

    //add data in sealedTsFiles and unSealedTsFile
    if (sortedSeriesDataSource.getSealedTsFiles() != null) {
      seriesReaders.add(new IoTDBSequenceDataReader.SealedTsFileWithFilterReader(sortedSeriesDataSource.getSealedTsFiles()));
    }
    if (sortedSeriesDataSource.getUnsealedTsFile() != null) {
      seriesReaders.add(new IoTDBSequenceDataReader.UnSealedTsFileWithFilterReader(sortedSeriesDataSource.getUnsealedTsFile()));
    }

    //add data in memTable
    if (sortedSeriesDataSource.hasRawSeriesChunk() && singleSeriesExpression == null) {
      seriesReaders.add(new RawSeriesChunkReaderWithoutFilter(sortedSeriesDataSource.getRawSeriesChunk()));
    }
    if (sortedSeriesDataSource.hasRawSeriesChunk() && singleSeriesExpression != null) {
      seriesReaders.add(new RawSeriesChunkReaderWithFilter(sortedSeriesDataSource.getRawSeriesChunk(), singleSeriesExpression.getFilter()));
    }
  }

  protected class SealedTsFileWithFilterReader extends SequenceDataReader.SealedTsFileReader {


    public SealedTsFileWithFilterReader(List<IntervalFileNode> sealedTsFiles) {
      super(sealedTsFiles);
    }

    protected boolean singleTsFileSatisfied(IntervalFileNode fileNode) {
      if (fileNode.getStartTime(path.getDevice()) == -1) {
        return false;
      }
      //no singleSeriesExpression
      if (singleSeriesExpression == null) {
        return true;
      }

      if (singleSeriesExpression.getType() == QueryFilterType.GLOBAL_TIME) {//singleSeriesExpression time
        DigestFilterVisitor digestFilterVisitor = new DigestFilterVisitor();
        DigestForFilter timeDigest = new DigestForFilter(fileNode.getStartTime(path.getDevice()),
                fileNode.getEndTime(path.getDeltaObjectToString()));
        return digestFilterVisitor.satisfy(timeDigest, null, singleSeriesExpression.getFilter());
      } else {//fileNode doesn't hold the value scope for series
        return true;
      }
    }

    protected void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {
      RandomAccessFile raf = OverflowFileStreamManager.getInstance().get(jobId, fileNode.getFilePath());
      ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(raf);

      if (singleSeriesExpression == null) {
        singleTsFileReader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, path);
      } else {
        singleTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader, path, singleSeriesExpression.getFilter());
      }

    }
  }

  protected class UnSealedTsFileWithFilterReader extends SequenceDataReader.UnSealedTsFileReader {
    public UnSealedTsFileWithFilterReader(UnsealedTsFile unsealedTsFile) throws IOException {
      super(unsealedTsFile);
    }

    protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
                                          SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
      if (singleSeriesExpression == null) {
        singleTsFileReader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
      } else {
        singleTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList, singleSeriesExpression.getFilter());
      }
    }
  }
}
