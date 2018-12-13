package cn.edu.tsinghua.iotdb.queryV2.engine;

import cn.edu.tsinghua.iotdb.queryV2.engine.component.executor.QueryJobExecutor;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;


public interface QueryJobDispatcher {

    QueryJobExecutor dispatch(QueryJob queryJob);

}
