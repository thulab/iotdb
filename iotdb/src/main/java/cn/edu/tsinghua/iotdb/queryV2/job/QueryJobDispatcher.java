package cn.edu.tsinghua.iotdb.queryV2.job;

import cn.edu.tsinghua.iotdb.queryV2.component.executor.QueryJobExecutor;
import cn.edu.tsinghua.iotdb.queryV2.component.job.QueryJob;


public interface QueryJobDispatcher {

    QueryJobExecutor dispatch(QueryJob queryJob);

}
