package cn.edu.tsinghua.tsfile.timeseries.read.common;

import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.nio.ByteBuffer;

/**
 * used in query
 */
public class Chunk {

    private ChunkHeader chunkHeader;
    private ByteBuffer chunkData;

    public Chunk(ChunkHeader header, ByteBuffer buffer) {
        this.chunkHeader = header;
        this.chunkData = buffer;
    }

    public ChunkHeader getHeader() {
        return chunkHeader;
    }

    public ByteBuffer getData() {
        return chunkData;
    }
}
