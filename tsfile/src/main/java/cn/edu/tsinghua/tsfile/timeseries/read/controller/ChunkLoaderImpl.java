package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.MemChunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Read one Chunk and cache it
 */
public class ChunkLoaderImpl implements ChunkLoader {
    private static final int DEFAULT_MEMSERISCHUNK_CACHE_SIZE = 100;
    private TsFileSequenceReader fileSequenceReader;
    private LRUCache<ChunkMetaData, ByteBuffer> chunkBytesCache;

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_MEMSERISCHUNK_CACHE_SIZE);
    }

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {
        this.fileSequenceReader = fileSequenceReader;
        chunkBytesCache = new LRUCache<ChunkMetaData, ByteBuffer>(cacheSize) {
            @Override
            public void beforeRemove(ByteBuffer object) {
                return;
            }

            @Override
            public ByteBuffer loadObjectByKey(ChunkMetaData key) throws CacheException {
                try {
                    return load(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    public MemChunk getMemChunk(ChunkMetaData chunkMetaData) throws IOException {
        try {
            chunkBytesCache.get(chunkMetaData).position(0);
            return new MemChunk(chunkMetaData, chunkBytesCache.get(chunkMetaData));
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private ByteBuffer load(ChunkMetaData chunkMetaData) throws IOException {
        return fileSequenceReader.readChunkAndHeader(chunkMetaData.getFileOffsetOfCorrespondingData());
    }
}
