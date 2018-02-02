package cn.edu.tsinghua.iotdb.queryV2.engine.component.job;

import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;

/**
 * Created by zhangjinrui on 2018/1/9.
 */
public interface QueryJobFuture {
    /**
     * Wait until corresponding QueryJob is finished
     */
    void waitToFinished();

    /**
     * Terminate corresponding QueryJob. This method is synchronized and
     * invoking will be blocked until corresponding QueryJob is terminated.
     */
    void terminateCurrentJob();

    /**
     * Get current status of corresponding QueryJob
     * @return status
     */
    QueryJobStatus getCurrentStatus();

    /**
     * Retrieve QueryDataSet from QueryEngine result pool.
     * @return null if the queryJob is not finished.
     */
    QueryDataSet retrieveQueryDataSet();

}
