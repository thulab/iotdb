package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.filter.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.impl.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.query.impl.QueryWithQueryFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.query.impl.QueryWithoutFilterExecutorImpl;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
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
                    return new QueryWithGlobalTimeFilterExecutorImpl(chunkLoader, metadataQuerier).execute(queryExpression);
                } else {
                    return new QueryWithQueryFilterExecutorImpl(chunkLoader, metadataQuerier).execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return new QueryWithoutFilterExecutorImpl(chunkLoader, metadataQuerier).execute(queryExpression);
        }
    }
}
