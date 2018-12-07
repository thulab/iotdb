package cn.edu.tsinghua.tsfile.timeseries.read.query.executor;

import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;

import java.io.IOException;


public interface QueryExecutor {

    QueryDataSet execute(QueryExpression queryExpression) throws IOException;
}
