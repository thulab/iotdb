package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithoutFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.GlobalTimeExpression;
import cn.edu.tsinghua.tsfile.read.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static cn.edu.tsinghua.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

public class QueryEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryEngine.class);

  public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression queryFilter = queryExpression.getExpression();
        IExpression expression = QueryFilterOptimizer.getInstance().
                optimize(queryFilter, queryExpression.getSelectedSeries());
        queryExpression.setExpression(expression);

        if (expression.getType() == GLOBAL_TIME) {
          return QueryWithGlobalTimeFilterExecutorImpl.execute(queryExpression);
        } else {
          return QueryWithFilterExecutorImpl.execute(queryExpression);
        }
      } catch (QueryFilterOptimizationException e) {
        throw new IOException(e);
      }
    } else {
      return QueryWithoutFilterExecutorImpl.execute(queryExpression);
    }
  }
}
