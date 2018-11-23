package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;

/**
 * Read one Chunk and cache it
 */
public class ChunkLoaderImpl implements ChunkLoader {
    private static final int DEFAULT_MEMSERISCHUNK_CACHE_SIZE = 100;
    private TsFileSequenceReader reader;
    private LRUCache<ChunkMetaData, Chunk> chunkCache;

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_MEMSERISCHUNK_CACHE_SIZE);
    }

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {

        this.reader = fileSequenceReader;

        chunkCache = new LRUCache<ChunkMetaData, Chunk>(cacheSize) {
            @Override
            public void beforeRemove(Chunk object) {
                return;
            }

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
            chunkCache.get(chunkMetaData).getData().position(0);
            return chunkCache.get(chunkMetaData);
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

}
