package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithFilter;

import java.io.IOException;
import java.util.List;


public class NodeConstructorForSingleFileImpl extends NodeConstructor {
    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;

    public NodeConstructorForSingleFileImpl(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.chunkLoader = chunkLoader;
    }

    @Override
    public Reader generateSeriesReader(SeriesFilter seriesFilter) throws IOException {
        List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(
                seriesFilter.getSeriesPath());
        return new SeriesReaderWithFilter(chunkLoader, chunkMetaDataList, seriesFilter.getFilter());
    }
}
