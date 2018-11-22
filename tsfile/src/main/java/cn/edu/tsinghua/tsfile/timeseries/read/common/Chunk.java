package cn.edu.tsinghua.tsfile.timeseries.read.common;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.nio.ByteBuffer;


public interface Chunk {

    ChunkMetaData getChunkMetaData();

    ByteBuffer getChunkBodyStream();
}
