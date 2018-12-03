package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.DataSetWithFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.DataSetWithoutFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithoutFilter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;


public class QueryExecutorRouter implements QueryExecutor {

    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;

    public QueryExecutorRouter(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.chunkLoader = chunkLoader;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        if (queryExpression.hasQueryFilter()) {
            try {
                QueryFilter queryFilter = queryExpression.getQueryFilter();
                QueryFilter regularQueryFilter = QueryFilterOptimizer.getInstance().convertGlobalTimeFilter(queryFilter, queryExpression.getSelectedSeries());
                queryExpression.setQueryFilter(regularQueryFilter);

                if (regularQueryFilter instanceof GlobalTimeFilter) {
                    return execute(queryExpression.getSelectedSeries(), ((GlobalTimeFilter) regularQueryFilter).getFilter());
                } else {
                    return executeWithFilter(queryExpression.getSelectedSeries(), queryExpression.getQueryFilter());
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return execute(queryExpression.getSelectedSeries());
        }
    }

    /**
     * Without time generator, with global time filter.
     */
    private QueryDataSet execute(List<Path> selectedPathList, Filter timeFilter) throws IOException {
        LinkedHashMap<Path, Reader> readersOfSelectedSeries = new LinkedHashMap<>();

        for (Path path : selectedPathList) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            Reader seriesReader = new SeriesReaderWithFilter(chunkLoader, chunkMetaDataList, timeFilter);
            readersOfSelectedSeries.put(path, seriesReader);
        }
        return new DataSetWithoutFilter(readersOfSelectedSeries, true);
    }

    /**
     * Without time generator, without filter.
     */
    private QueryDataSet execute(List<Path> selectedPathList) throws IOException {
        LinkedHashMap<Path, Reader> readersOfSelectedSeries = new LinkedHashMap<>();

        for (Path path : selectedPathList) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            Reader seriesReader = new SeriesReaderWithoutFilter(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.put(path, seriesReader);
        }
        return new DataSetWithoutFilter(readersOfSelectedSeries, true);
    }

    /**
     * With time generator.
     */
    public QueryDataSet executeWithFilter(List<Path> selectedPathList, QueryFilter queryFilter) throws IOException {
        TimestampGenerator timestampGenerator = new TimestampGeneratorByQueryFilterImpl(queryFilter, chunkLoader, metadataQuerier);
        LinkedHashMap<Path, SeriesReaderByTimestamp> readersOfSelectedSeries = new LinkedHashMap<>();

        for (Path path : selectedPathList) {
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            SeriesReaderByTimestamp seriesReader = new SeriesReaderByTimestamp(
                    chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.put(path, seriesReader);
        }
        return new DataSetWithFilter(timestampGenerator, readersOfSelectedSeries);
    }
}
