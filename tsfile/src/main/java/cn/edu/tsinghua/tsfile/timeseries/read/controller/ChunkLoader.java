package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.io.IOException;


public interface ChunkLoader {

    /**
     * read all content of any chunk
     */
    Chunk getMemChunk(ChunkMetaData chunkMetaData) throws IOException;
}
