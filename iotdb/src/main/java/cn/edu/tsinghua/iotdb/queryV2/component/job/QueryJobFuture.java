package cn.edu.tsinghua.iotdb.queryV2.component.job;

import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;


public interface QueryJobFuture {

    /**
     * Wait until corresponding QueryJob is finished.
     * This method is synchronized and invoking this method will block current thread.
     * An InterruptedException will be thrown if current thread is interrupted.
     */
    void waitToFinished() throws InterruptedException;

    /**
     * Terminate corresponding QueryJob. This method is synchronized and
     * invoking will be blocked until corresponding QueryJob is terminated.
     * This method is synchronized and invoking this method will block current thread.
     * An InterruptedException will be thrown if current thread is interrupted.
     */
    void terminateCurrentJob() throws InterruptedException;

    /**
     * Get current status of corresponding QueryJob
     * @return status
     */
    QueryJobStatus getCurrentStatus();

    /**
     * Retrieve OnePassQueryDataSet from EngineQueryExecutor result pool.
     * @return null if the queryJob is not finished.
     */
    QueryDataSet retrieveQueryDataSet();
}