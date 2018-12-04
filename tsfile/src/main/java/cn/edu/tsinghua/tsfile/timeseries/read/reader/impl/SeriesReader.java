package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public abstract class SeriesReader implements Reader {

    protected ChunkLoader chunkLoader;
    protected List<ChunkMetaData> chunkMetaDataList;

    protected ChunkReader chunkReader;
    protected boolean seriesChunkReaderInitialized;
    protected int currentReadSeriesChunkIndex;

    protected TsFileSequenceReader fileReader;

    public SeriesReader(TsFileSequenceReader fileReader, Path path) throws IOException {
        this.fileReader = fileReader;
        this.chunkLoader = new ChunkLoaderImpl(fileReader);
        this.chunkMetaDataList = new MetadataQuerierByFileImpl(fileReader).getChunkMetaDataList(path);
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    public SeriesReader(TsFileSequenceReader fileReader,
                        ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this(chunkLoader, chunkMetaDataList);
        this.fileReader = fileReader;
    }

    /**
     * Using this constructor cannot close corresponding FileStream
     * @param chunkLoader
     * @param chunkMetaDataList
     */
    public SeriesReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (seriesChunkReaderInitialized && chunkReader.hasNext()) {
            return true;
        }
        while ((currentReadSeriesChunkIndex + 1) < chunkMetaDataList.size()) {
            if (!seriesChunkReaderInitialized) {
                ChunkMetaData chunkMetaData = chunkMetaDataList.get(++currentReadSeriesChunkIndex);
                if (seriesChunkSatisfied(chunkMetaData)) {
                    initSeriesChunkReader(chunkMetaData);
                    seriesChunkReaderInitialized = true;
                } else {
                    continue;
                }
            }
            if (chunkReader.hasNext()) {
                return true;
            } else {
                seriesChunkReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return chunkReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    protected abstract void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException;

    protected abstract boolean seriesChunkSatisfied(ChunkMetaData chunkMetaData);

    public void close() throws IOException {
        if (fileReader != null) {
            fileReader.close();
        }
    }
}
