package cn.edu.tsinghua.tsfile.read.query.executor;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderByTimestamp;
import cn.edu.tsinghua.tsfile.read.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.read.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.read.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGenerator;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGeneratorImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ExecutorWithTimeGenerator implements QueryExecutor {

    private MetadataQuerier metadataQuerier;
    private ChunkLoader chunkLoader;

    public ExecutorWithTimeGenerator(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.chunkLoader = chunkLoader;
    }


    /**
     * All leaf nodes of queryFilter in queryExpression are SeriesFilters,
     * We use a TimeGenerator to control query processing.
     *
     * for more information, see DataSetWithTimeGenerator
     *
     * @return DataSet with TimeGenerator
     */
    public DataSetWithTimeGenerator execute(QueryExpression queryExpression) throws IOException {

        QueryFilter queryFilter = queryExpression.getQueryFilter();
        List<Path> selectedPathList = queryExpression.getSelectedSeries();

        // get TimeGenerator by queryFilter
        TimeGenerator timeGenerator = new TimeGeneratorImpl(queryFilter, chunkLoader, metadataQuerier);

        // the size of hasFilter is equal to selectedPathList, if a series has a filter, it is true, otherwise false
        List<Boolean> cached = removeFilteredPaths(queryFilter, selectedPathList);
        List<SeriesReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        for(int i = 0; i < cached.size(); i++) {

            List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(selectedPathList.get(i));
            dataTypes.add(chunkMetaDataList.get(0).getTsDataType());

            if(cached.get(i)) {
                readersOfSelectedSeries.add(null);
                continue;
            }

            SeriesReaderByTimestamp seriesReader = new SeriesReaderByTimestamp(chunkLoader, chunkMetaDataList);
            readersOfSelectedSeries.add(seriesReader);
        }

        return new DataSetWithTimeGenerator(selectedPathList, cached, dataTypes, timeGenerator, readersOfSelectedSeries);
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
