package cn.edu.tsinghua.tsfile.timeseries.read.query.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.DataSetwithoutTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class QueryWithoutFilterExecutorImpl implements QueryExecutor {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithoutFilterExecutorImpl(ChunkLoader chunkLoader, MetadataQuerier metadataQuerier) {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        LinkedHashMap<Path, Reader> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new DataSetwithoutTimeGenerator(readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, Reader> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException {
        for (Path path : selectedSeries) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            Reader seriesReader = new SeriesReaderWithoutFilter(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}
