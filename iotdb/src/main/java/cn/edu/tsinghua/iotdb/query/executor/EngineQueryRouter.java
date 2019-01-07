package cn.edu.tsinghua.iotdb.query.executor;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.control.QueryTokenManager;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.util.ExpressionOptimizer;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static cn.edu.tsinghua.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

/**
 * <p> Query entrance class of IoTDB query process.
 * All query clause will be transformed to physical plan, physical plan will be executed by EngineQueryRouter.
 */
public class EngineQueryRouter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineQueryRouter.class);

    /**
     * Each unique jdbc request(query, aggregation or others job) has an unique job id.
     * This job id will always be maintained until the request is closed.
     * In each job, the unique file will be only opened once to avoid too many opened files error.
     */
    private AtomicLong jobId = new AtomicLong();

    public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        long jobId = getNextJobId();
        QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(jobId);

        if (queryExpression.hasQueryFilter()) {
            try {
                IExpression optimizedExpression = ExpressionOptimizer.getInstance().
                        optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
                queryExpression.setExpression(optimizedExpression);

                if (optimizedExpression.getType() == GLOBAL_TIME) {
                    EngineExecutorWithoutTimeGenerator engineExecutor = new EngineExecutorWithoutTimeGenerator(queryExpression);
                    return engineExecutor.executeWithGlobalTimeFilter(jobId);
                } else {
                    EngineExecutorWithTimeGenerator engineExecutor = new EngineExecutorWithTimeGenerator(queryExpression);
                    return engineExecutor.execute(jobId);
                }

            } catch (QueryFilterOptimizationException | PathErrorException e) {
                throw new IOException(e);
            }
        } else {
            try {
                EngineExecutorWithoutTimeGenerator engineExecutor = new EngineExecutorWithoutTimeGenerator(queryExpression);
                return engineExecutor.executeWithoutFilter(jobId);
            } catch (PathErrorException e) {
                throw new IOException(e);
            }
        }
    }

    private synchronized long getNextJobId() {
        return jobId.incrementAndGet();
    }
}
