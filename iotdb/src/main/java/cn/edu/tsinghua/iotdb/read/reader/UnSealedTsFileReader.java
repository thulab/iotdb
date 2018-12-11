package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.List;

public class UnSealedTsFileReader implements ISeriesReader {

  protected Path seriesPath;
  private FileSeriesReader tsFileReader;
  private BatchData data;
  private SingleSeriesExpression singleSeriesExpression;

  public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile) throws IOException {

    ChunkLoader chunkLoader = new ChunkLoaderImpl(new TsFileSequenceReader(unsealedTsFile.getFilePath(), false));

    initSingleTsFileReader(chunkLoader, unsealedTsFile.getTimeSeriesChunkMetaDatas());
  }

  public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile, SingleSeriesExpression singleSeriesExpression) throws IOException {
    this(unsealedTsFile);

    this.singleSeriesExpression = singleSeriesExpression;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data == null)
      data = tsFileReader.nextBatch();

    if (!data.hasNext() && !tsFileReader.hasNextBatch())
      return false;

    while (!data.hasNext()) {
      data = tsFileReader.nextBatch();
      if (data.hasNext())
        return true;
    }

    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    switch (data.getDataType()) {
      case INT32:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsInt(data.getInt()));
      case INT64:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsLong(data.getLong()));
      case FLOAT:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsFloat(data.getFloat()));
      case DOUBLE:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsDouble(data.getDouble()));
      case TEXT:
        return new TimeValuePair(data.currentTime(), new TsPrimitiveType.TsBinary(data.getBinary()));
      default:
        throw new UnSupportedDataTypeException(String.valueOf(data.getDataType()));
    }
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    data.next();
  }

  @Override
  public void close() throws IOException {
    if (tsFileReader != null) {
      tsFileReader.close();
    }
  }

  protected void initSingleTsFileReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaData) {
    if (singleSeriesExpression == null) {
      tsFileReader = new FileSeriesReaderWithoutFilter(chunkLoader, chunkMetaData);
    } else {
      tsFileReader = new FileSeriesReaderWithFilter(chunkLoader, chunkMetaData, singleSeriesExpression.getFilter());
    }
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
