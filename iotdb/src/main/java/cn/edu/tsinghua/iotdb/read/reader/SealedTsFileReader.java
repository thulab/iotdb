package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.List;

public class SealedTsFileReader implements ISeriesReader {
  
  private Path seriesPath;
  private List<IntervalFileNode> sealedTsFiles;
  private int usedIntervalFileIndex;
  private FileSeriesReader singleTsFileReader;
  private boolean singleTsFileReaderInitialized;
  private SingleSeriesExpression singleSeriesExpression;
  private BatchData data;


  public SealedTsFileReader(GlobalSortedSeriesDataSource sortedSeriesDataSource) {
    this.seriesPath = sortedSeriesDataSource.getSeriesPath();
    this.sealedTsFiles = sortedSeriesDataSource.getSealedTsFiles();
    this.usedIntervalFileIndex = -1;
    this.singleTsFileReader = null;
    this.singleTsFileReaderInitialized = false;
  }

  public SealedTsFileReader(GlobalSortedSeriesDataSource sortedSeriesDataSource, SingleSeriesExpression singleSeriesExpression) {
    this(sortedSeriesDataSource);
    this.singleSeriesExpression = singleSeriesExpression;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (singleTsFileReaderInitialized && singleTsFileReader.hasNextBatch()) {
      return true;
    }
    while ((usedIntervalFileIndex + 1) < sealedTsFiles.size()) {
      if (!singleTsFileReaderInitialized) {
        IntervalFileNode fileNode = sealedTsFiles.get(++usedIntervalFileIndex);
        if (singleTsFileSatisfied(fileNode)) {
          initSingleTsFileReader(fileNode);
          singleTsFileReaderInitialized = true;
        } else {
          continue;
        }
      }
      if (singleTsFileReader.hasNextBatch()) {
        return true;
      } else {
        singleTsFileReaderInitialized = false;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    return singleTsFileReader.next();
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    next();
  }

  @Override
  public void close() throws IOException {
    if (singleTsFileReader != null) {
      singleTsFileReader.close();
    }
  }

  public boolean singleTsFileSatisfied(IntervalFileNode fileNode) {
    
    if (singleSeriesExpression == null) 
      return true;
    
    // TODO
    
    return true;
  }

  public void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {
    TsFileSequenceReader tsFileReader = new TsFileSequenceReader(fileNode.getFilePath(), true);
    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);
    
    if (singleSeriesExpression == null) {
      singleTsFileReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      singleTsFileReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, singleSeriesExpression.getFilter());
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
