package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;

import java.io.IOException;


public interface QueryExecutor {

    QueryDataSet execute(QueryExpression queryExpression) throws IOException;
}
