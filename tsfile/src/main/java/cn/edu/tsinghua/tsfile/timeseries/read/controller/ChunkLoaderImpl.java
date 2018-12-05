package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.nio.ByteBuffer;

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
            public Chunk loadObjectByKey(ChunkMetaData metaData) throws CacheException {
                try {
                    return reader.readMemChunk(metaData);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    public Chunk getChunk(ChunkMetaData chunkMetaData) throws IOException {
        try {
            Chunk chunk = chunkCache.get(chunkMetaData);
            return new Chunk(chunk.getHeader(), chunk.getData().duplicate());
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        reader.close();
    }

}
