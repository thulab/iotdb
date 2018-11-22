package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public abstract class SeriesReaderFromSingleFile implements SeriesReader {

    protected ChunkLoader chunkLoader;
    protected List<ChunkMetaData> chunkMetaDataList;

    protected SeriesChunkReader seriesChunkReader;
    protected boolean seriesChunkReaderInitialized;
    protected int currentReadSeriesChunkIndex;

    protected TsFileSequenceReader fileReader;

    public SeriesReaderFromSingleFile(TsFileSequenceReader fileReader, Path path) throws IOException {
        this.fileReader = fileReader;
        this.chunkLoader = new ChunkLoaderImpl(fileReader);
        this.chunkMetaDataList = new MetadataQuerierByFileImpl(fileReader).getChunkMetaDataList(path);
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    public SeriesReaderFromSingleFile(TsFileSequenceReader fileReader,
                                      ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this(chunkLoader, chunkMetaDataList);
        this.fileReader = fileReader;
    }

    /**
     * Using this constructor cannot close corresponding FileStream
     * @param chunkLoader
     * @param chunkMetaDataList
     */
    public SeriesReaderFromSingleFile(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (seriesChunkReaderInitialized && seriesChunkReader.hasNext()) {
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
            if (seriesChunkReader.hasNext()) {
                return true;
            } else {
                seriesChunkReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return seriesChunkReader.next();
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
