package cn.edu.tsinghua.tsfile.timeseries.read.common;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MemChunk implements Chunk {
    private ChunkMetaData chunkMetaData;
    private ByteBuffer chunkBodyStream;

    public MemChunk(ChunkMetaData chunkMetaData, ByteBuffer chunkBodyStream) {
        this.chunkMetaData = chunkMetaData;
        this.chunkBodyStream = chunkBodyStream;
    }

    public ChunkMetaData getChunkMetaData() {
        return chunkMetaData;
    }

    public ByteBuffer getChunkBodyStream() {
        return chunkBodyStream;
    }
}
