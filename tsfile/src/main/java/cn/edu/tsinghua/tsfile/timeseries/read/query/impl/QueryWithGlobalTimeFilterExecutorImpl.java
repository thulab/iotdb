package cn.edu.tsinghua.tsfile.timeseries.read.query.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithFilter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class QueryWithGlobalTimeFilterExecutorImpl implements QueryExecutor {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithGlobalTimeFilterExecutorImpl(ChunkLoader chunkLoader, MetadataQuerier metadataQuerier) {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        LinkedHashMap<Path, Reader> readersOfSelectedSeries = new LinkedHashMap<>();
        Filter timeFilter = ((GlobalTimeFilter) queryExpression.getQueryFilter()).getFilter();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), timeFilter);
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, Reader> readersOfSelectedSeries,
                                             List<Path> selectedSeries, Filter timeFilter) throws IOException {
        for (Path path : selectedSeries) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            Reader seriesReader = new SeriesReaderWithFilter(chunkLoader, chunkMetaDataList, timeFilter);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}
