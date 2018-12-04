package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.DynamicOneColumnData;
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

    public SeriesReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.currentChunkIndex = 0;
        this.chunkReaderInitialized = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (chunkReaderInitialized && chunkReader.hasNext()) {
            return true;
        }
        while (currentChunkIndex < chunkMetaDataList.size()) {
            if (!chunkReaderInitialized) {
                ChunkMetaData chunkMetaData = chunkMetaDataList.get(currentChunkIndex++);
                if (chunkSatisfied(chunkMetaData)) {
                    initSeriesChunkReader(chunkMetaData);
                    chunkReaderInitialized = true;
                } else {
                    continue;
                }
            }
            if (chunkReader.hasNext()) {
                return true;
            } else {
                chunkReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return chunkReader.next();
    }

    @Override
    public boolean hasNextBatch() throws IOException {

        // current chunk has additional batch
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            return true;
        }

        // current chunk does not have additional batch, init new chunk reader
        while (currentChunkIndex < chunkMetaDataList.size()) {

            ChunkMetaData chunkMetaData = chunkMetaDataList.get(currentChunkIndex++);
            if (chunkSatisfied(chunkMetaData)) {
                // chunk metadata satisfy the condition
                initSeriesChunkReader(chunkMetaData);

                if (chunkReader.hasNextBatch())
                    return true;
            }

        }

        return false;
    }

    @Override
    public DynamicOneColumnData nextBatch() {
        return chunkReader.nextBatch();
    }


    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    protected abstract void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException;

    protected abstract boolean chunkSatisfied(ChunkMetaData chunkMetaData);

    public void close() throws IOException {
        chunkLoader.close();
    }

}
