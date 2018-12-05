package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/13.
 */
public interface QueryExecutor {

    QueryDataSet execute(QueryExpression queryExpression) throws IOException;
}
