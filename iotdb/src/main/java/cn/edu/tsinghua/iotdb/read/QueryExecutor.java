package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;

public interface QueryExecutor {
    QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException;
}