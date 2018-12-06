package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.DataSetWithTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ExecutorWithTimeGenerator implements QueryExecutor{

    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;

    public ExecutorWithTimeGenerator(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.chunkLoader = chunkLoader;
    }


    /**
     * Has SeriesFilters, need to generate time by these filters first
     * all leaf nodes of queryFilter in queryExpression are SeriesFilters
     *
     * @return DataSet with TimeGenerator
     */
    public DataSetWithTimeGenerator execute(QueryExpression queryExpression) throws IOException {

        QueryFilter queryFilter = queryExpression.getQueryFilter();
        List<Path> selectedPathList = queryExpression.getSelectedSeries();

        List<Boolean> cached = removeFilteredPaths(queryFilter, selectedPathList);

        TimestampGenerator timestampGenerator = new TimestampGeneratorByQueryFilterImpl(queryFilter, chunkLoader, metadataQuerier);

        List<SeriesReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for (Path path : selectedPathList) {
            if (path == null) {
                readersOfSelectedSeries.add(null);
                continue;
            }
            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
            SeriesReaderByTimestamp seriesReader = new SeriesReaderByTimestamp(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.add(seriesReader);
            dataTypes.add(chunkMetaDataList.get(0).getTsDataType());
        }
        return new DataSetWithTimeGenerator(selectedPathList, cached, dataTypes, timestampGenerator, readersOfSelectedSeries);
    }

    private List<Boolean> removeFilteredPaths(QueryFilter queryFilter, List<Path> selectedPaths) {

        List<Boolean> cached = new ArrayList<>();
        HashSet<Path> filteredPaths = new HashSet<>();
        getAllFilteredPaths(queryFilter, filteredPaths);

        for (Path selectedPath : selectedPaths) {
            cached.add(filteredPaths.contains(selectedPath));
        }

        return cached;

    }

    private void getAllFilteredPaths(QueryFilter queryFilter, HashSet<Path> paths) {
        if (queryFilter instanceof QueryFilterFactory) {
            getAllFilteredPaths(((QueryFilterFactory) queryFilter).getLeft(), paths);
            getAllFilteredPaths(((QueryFilterFactory) queryFilter).getRight(), paths);
        } else if (queryFilter instanceof SeriesFilter) {
            paths.add(((SeriesFilter) queryFilter).getSeriesPath());
        }
    }

}
