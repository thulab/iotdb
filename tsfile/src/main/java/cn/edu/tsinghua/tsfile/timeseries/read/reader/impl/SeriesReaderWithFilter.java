package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;

import java.io.IOException;
import java.util.List;


public class SeriesReaderWithFilter extends SeriesReader {

    private Filter filter;

    public SeriesReaderWithFilter(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList, Filter filter) {
        super(chunkLoader, chunkMetaDataList);
        this.filter = filter;
    }

    public SeriesReaderWithFilter(TsFileSequenceReader tsFileReader, ChunkLoader chunkLoader,
                                  List<ChunkMetaData> chunkMetaDataList, Filter filter) {
        super(tsFileReader, chunkLoader, chunkMetaDataList);
        this.filter = filter;
    }

    public SeriesReaderWithFilter(TsFileSequenceReader tsFileReader
            , Path path, Filter filter) throws IOException {
        super(tsFileReader, path);
        this.filter = filter;
    }

    protected void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderWithFilter(chunk, filter);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(ChunkMetaData chunkMetaData) {
        DigestForFilter digest = new DigestForFilter(
                chunkMetaData.getStartTime(),
                chunkMetaData.getEndTime(),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                chunkMetaData.getTsDataType());
        return filter.satisfy(digest);
    }
}
