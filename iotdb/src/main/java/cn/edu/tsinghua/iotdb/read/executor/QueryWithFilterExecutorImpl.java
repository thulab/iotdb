package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.QueryDataSetForQueryWithQueryFilterImpl;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.QueryByTimestampsReader;
import cn.edu.tsinghua.iotdb.read.timegenerator.IoTTimeGenerator;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.ExpressionType;
import cn.edu.tsinghua.tsfile.read.expression.IBinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.TimeGenerator;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderByTimestamp;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor with filter
 */
public class QueryWithFilterExecutorImpl {

    public QueryWithFilterExecutorImpl() {
    }

    public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        TimeGenerator timestampGenerator = new IoTTimeGenerator(queryExpression.getExpression());

        LinkedHashMap<Path, QueryByTimestampsReader> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), queryExpression.getExpression());
        return new QueryDataSetForQueryWithQueryFilterImpl(timestampGenerator, readersOfSelectedSeries);
    }

    private static void initReadersOfSelectedSeries(LinkedHashMap<Path, QueryByTimestampsReader> readersOfSelectedSeries,
                                                    List<Path> selectedSeries, IExpression expression) throws IOException, FileNodeManagerException {

        LinkedHashMap<Path, SingleSeriesExpression> path2SeriesFilter = parseSeriesFilter(expression);

        for (Path path : selectedSeries) {
            QueryDataSource queryDataSource = null;
            if (path2SeriesFilter.containsKey(path)) {
                queryDataSource = QueryDataSourceExecutor.getQueryDataSource(path2SeriesFilter.get(path));
            } else {
                queryDataSource = QueryDataSourceExecutor.getQueryDataSource(path);
            }
            QueryByTimestampsReader seriesReader = new QueryByTimestampsReader(queryDataSource);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }

    private static LinkedHashMap parseSeriesFilter(IExpression expression) {
        LinkedHashMap<Path, SingleSeriesExpression> path2SeriesFilter = new LinkedHashMap<>();
        traverse(expression, path2SeriesFilter);
        return path2SeriesFilter;
    }

    private static void traverse(IExpression expression, LinkedHashMap path2SeriesFilter) {
        if (expression.getType() == ExpressionType.SERIES) {
            SingleSeriesExpression seriesFilter = (SingleSeriesExpression) expression;
            path2SeriesFilter.put(seriesFilter.getSeriesPath(), seriesFilter);
        } else {
            IBinaryExpression binaryQueryFilter = (IBinaryExpression) expression;
            traverse(binaryQueryFilter.getLeft(), path2SeriesFilter);
            traverse(binaryQueryFilter.getRight(), path2SeriesFilter);
        }
    }
}
