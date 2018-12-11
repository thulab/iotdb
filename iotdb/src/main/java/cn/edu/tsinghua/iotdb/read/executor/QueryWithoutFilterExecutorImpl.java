package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.QueryReader;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.query.impl.MergeQueryDataSet;
import cn.edu.tsinghua.tsfile.read.reader.SeriesReader;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor without filter
 * */
public class QueryWithoutFilterExecutorImpl {

    public QueryWithoutFilterExecutorImpl() {
    }

    public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
        LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private static void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException, FileNodeManagerException {
        for (Path path : selectedSeries) {
            QueryDataSource queryDataSource = QueryDataSourceExecutor.getQueryDataSource(path);
            SeriesReader seriesReader = new QueryReader(queryDataSource);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}
