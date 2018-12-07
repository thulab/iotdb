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
    private int chunkToRead;

    private BatchData data;

    public SeriesReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.chunkToRead = 0;
    }

    @Override
    public boolean hasNextBatch() {

        // current chunk has data
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            return true;

            // has additional chunk to read
        } else {
            return chunkToRead < chunkMetaDataList.size();
        }

    }

    @Override
    public BatchData nextBatch() throws IOException {

        // current chunk has additional batch
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            data = chunkReader.nextBatch();
            return data;
        }

        // current chunk does not have additional batch, init new chunk reader
        while (chunkToRead < chunkMetaDataList.size()) {

            ChunkMetaData chunkMetaData = chunkMetaDataList.get(chunkToRead++);
            if (chunkSatisfied(chunkMetaData)) {
                // chunk metadata satisfy the condition
                initChunkReader(chunkMetaData);

                if (chunkReader.hasNextBatch()) {
                    data = chunkReader.nextBatch();
                    return data;
                }
            }
        }

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
