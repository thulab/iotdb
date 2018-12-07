package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;

/**
 * Read one Chunk and cache it into a LRUCache
 */
public class ChunkLoaderImpl implements ChunkLoader {
    private static final int DEFAULT_CHUNK_CACHE_SIZE = 100000;
    private TsFileSequenceReader reader;
    private LRUCache<ChunkMetaData, Chunk> chunkCache;

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_CHUNK_CACHE_SIZE);
    }

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {

        this.reader = fileSequenceReader;

        chunkCache = new LRUCache<ChunkMetaData, Chunk>(cacheSize) {

            @Override
            public Chunk loadObjectByKey(ChunkMetaData metaData) throws IOException {
                return reader.readMemChunk(metaData);
            }
        };
    }

    public Chunk getChunk(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkCache.get(chunkMetaData);
        return new Chunk(chunk.getHeader(), chunk.getData().duplicate());
    }

    public void close() throws IOException {
        reader.close();
    }

}
