package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithFilterImpl extends SeriesReaderFromSingleFile {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;

    public SeriesReaderFromSingleFileWithFilterImpl(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList, Filter<?> filter) {
        super(chunkLoader, chunkMetaDataList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    public SeriesReaderFromSingleFileWithFilterImpl(TsFileSequenceReader tsFileReader, ChunkLoader chunkLoader,
                                                    List<ChunkMetaData> chunkMetaDataList, Filter<?> filter) {
        super(tsFileReader, chunkLoader, chunkMetaDataList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    public SeriesReaderFromSingleFileWithFilterImpl(TsFileSequenceReader tsFileReader
            , Path path, Filter<?> filter) throws IOException {
        super(tsFileReader, path);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    protected void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.seriesChunkReader = new SeriesChunkReaderWithFilterImpl(chunk, filter);
        this.seriesChunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(ChunkMetaData chunkMetaData) {
        DigestForFilter timeDigest = new DigestForFilter(chunkMetaData.getStartTime(),
                chunkMetaData.getEndTime());
        DigestForFilter valueDigest = new DigestForFilter(
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                chunkMetaData.getTsDataType());
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }
}
