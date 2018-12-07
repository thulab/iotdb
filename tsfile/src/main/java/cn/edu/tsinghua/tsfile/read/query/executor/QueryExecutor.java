package cn.edu.tsinghua.tsfile.read.query.executor;

import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.query.QueryExpression;

import java.io.IOException;


public interface QueryExecutor {

    QueryDataSet execute(QueryExpression queryExpression) throws IOException;
}
