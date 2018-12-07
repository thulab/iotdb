package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.List;

/**
 * <p> Series reader is used to query one series of one tsfile.
 */
public abstract class SeriesReader implements Reader {

    protected ChunkLoader chunkLoader;
    protected List<ChunkMetaData> chunkMetaDataList;
    protected ChunkReader chunkReader;
    protected boolean chunkReaderInitialized;
    protected int currentChunkIndex;

    private BatchData data;
    private boolean hasCachedData;

    public SeriesReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.currentChunkIndex = 0;
        this.chunkReaderInitialized = false;
    }

    @Override
    public boolean hasNextBatch() throws IOException {

        if(hasCachedData)
            return true;

        // current chunk has additional batch
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            data = chunkReader.nextBatch();
            hasCachedData = true;
            return true;
        }

        // current chunk does not have additional batch, init new chunk reader
        while (currentChunkIndex < chunkMetaDataList.size()) {

            ChunkMetaData chunkMetaData = chunkMetaDataList.get(currentChunkIndex++);
            if (chunkSatisfied(chunkMetaData)) {
                // chunk metadata satisfy the condition
                initChunkReader(chunkMetaData);

                if (chunkReader.hasNextBatch()) {
                    this.data = chunkReader.nextBatch();
                    hasCachedData = true;
                    return true;
                }
            }

        }

        return false;
    }

    @Override
    public BatchData nextBatch() {
        hasCachedData = false;
        return data;
    }

    @Override
    public BatchData currentBatch() {
        return data;
    }

    protected abstract void initChunkReader(ChunkMetaData chunkMetaData) throws IOException;

    protected abstract boolean chunkSatisfied(ChunkMetaData chunkMetaData);

    public void close() throws IOException {
        chunkLoader.close();
    }

}
