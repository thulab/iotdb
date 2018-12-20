package cn.edu.tsinghua.iotdb.query.executor;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.util.ExpressionOptimizer;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static cn.edu.tsinghua.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

public class EngineQueryRouter {

  private static final Logger LOGGER = LoggerFactory.getLogger(EngineQueryRouter.class);

  public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
    if (queryExpression.hasQueryFilter()) {
      try {

        IExpression optimizedExpression = ExpressionOptimizer.getInstance().
                optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
        queryExpression.setExpression(optimizedExpression);

        if (optimizedExpression.getType() == GLOBAL_TIME) {
          return EngineExecutorWithoutTimeGenerator.executeWithGlobalTimeFilter(queryExpression);
        } else {
          return EngineExecutorWithTimeGenerator.execute(queryExpression);
        }

      } catch (QueryFilterOptimizationException | PathErrorException e) {
        throw new IOException(e);
      }
    } else {
      try {
        return EngineExecutorWithoutTimeGenerator.executeWithoutFilter(queryExpression);
      } catch (PathErrorException e) {
        throw new IOException(e);
      }
    }
  }

}
