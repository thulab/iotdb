//package cn.edu.tsinghua.iotdb.query.factory;
//
//import cn.edu.tsinghua.iotdb.query.control.FileStreamManager;
//import cn.edu.tsinghua.iotdb.query.reader.component.BufferedSeriesChunk;
//import cn.edu.tsinghua.iotdb.query.reader.component.SegmentInputStream;
//import cn.edu.tsinghua.iotdb.query.reader.component.SegmentInputStreamWithMMap;
//import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
//import cn.edu.tsinghua.tsfile.read.common.Chunk;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.MappedByteBuffer;
//
///**
// * This class is used to load one SeriesChunk according to the ChunkMetaData
// */
//public class OverflowSeriesChunkLoader {
//  private static final Logger logger = LoggerFactory.getLogger(OverflowSeriesChunkLoader.class);
//  private FileStreamManager overflowFileStreamManager;
//
//  public OverflowSeriesChunkLoader() {
//    overflowFileStreamManager = FileStreamManager.getInstance();
//  }
//
//  public Chunk getChunk(Long jobId, ChunkMetaData metaData) throws IOException {
//
//
//
//    if (overflowFileStreamManager.contains(metaData.getFilePath()) || (new File(metaData.getFilePath()).length() +
//            overflowFileStreamManager.getMappedByteBufferUsage().get() < Integer.MAX_VALUE)) {
//      MappedByteBuffer buffer = overflowFileStreamManager.get(metaData.getFilePath());
//      return new BufferedSeriesChunk(
//              new SegmentInputStreamWithMMap(buffer, metaData.getOffsetInFile(), metaData.getLengthOfBytes()),
//              metaData);
//    } else {
//      RandomAccessFile randomAccessFile = overflowFileStreamManager.get(jobId, metaData.getFilePath());
//      return new BufferedSeriesChunk(
//              new SegmentInputStream(randomAccessFile, metaData.getOffsetInFile(), metaData.getLengthOfBytes()),
//              metaData);
//    }
//  }
//}
