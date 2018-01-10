package cn.edu.tsinghua.iotdb.queryV2.engine.component;

import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;

/**
 * Created by zhangjinrui on 2018/1/9.
 */
public class QueryJob {

    private long jobId;
    private long submitTimestamp;
    private long startTimestamp;
    private long endTimestamp;
    private QueryJobStatus status;
    private QueryJobExecutionMessage message;

    private String clientId;

    private QueryExpression queryExpression;

    public QueryJob(long jobId) {
        this.jobId = jobId;
    }

    public QueryJobStatus getStatus() {
        return status;
    }

    public void setStatus(QueryJobStatus status) {
        this.status = status;
    }

    public int hashCode() {
        return Long.hashCode(jobId);
    }

    public boolean equals(Object o) {
        if (o instanceof QueryJob && ((QueryJob) o).getJobId() == jobId) {
            return true;
        }
        return false;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getJobId() {
        return jobId;
    }

    public long getSubmitTimestamp() {
        return submitTimestamp;
    }

    public void setSubmitTimestamp(long submitTimestamp) {
        this.submitTimestamp = submitTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public QueryExpression getQueryExpression() {
        return queryExpression;
    }

    public void setQueryExpression(QueryExpression queryExpression) {
        this.queryExpression = queryExpression;
    }

    public QueryJobExecutionMessage getMessage() {
        return message;
    }

    public void setMessage(QueryJobExecutionMessage message) {
        this.message = message;
    }

    public String toString() {
        return String.valueOf(jobId);
    }
}
