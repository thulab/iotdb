package cn.edu.tsinghua.tsfile.timeseries.read.query.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.DataSetWithTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * @author Jinrui Zhang
 */
public class QueryWithQueryFilterExecutorImpl implements QueryExecutor {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;

    public QueryWithQueryFilterExecutorImpl(ChunkLoader chunkLoader, MetadataQuerier metadataQuerier) {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        TimestampGenerator timestampGenerator = new TimestampGeneratorByQueryFilterImpl(queryExpression.getQueryFilter(),
                chunkLoader, metadataQuerier);
        LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new DataSetWithTimeGenerator(timestampGenerator, readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException {
        for (Path path : selectedSeries) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            SeriesReaderByTimestamp seriesReader = new SeriesReaderByTimestamp(
                    chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}