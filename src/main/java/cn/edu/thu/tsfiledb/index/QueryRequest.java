package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.List;

/**
 * The skeleton abstract class for a query request with specific parameters.
 *
 * @author Jiaye Wu
 */
public abstract class QueryRequest {

    protected String nameSpacePath;

    protected long startTime;

    protected long endTime;

    protected List<Pair<Long, Double>> querySeries;

    protected QueryRequest(String nameSpacePath, long startTime, long endTime, List<Pair<Long, Double>> querySeries) {
        this.nameSpacePath = nameSpacePath;
        this.startTime = startTime;
        this.endTime = endTime;
        this.querySeries = querySeries;
    }

    protected QueryRequest(String nameSpacePath, List<Pair<Long, Double>> querySeries) {
        this.nameSpacePath = nameSpacePath;
        this.startTime = Long.MIN_VALUE;
        this.endTime = Long.MAX_VALUE;
        this.querySeries = querySeries;
    }

    public String getNameSpacePath() {
        return nameSpacePath;
    }

    public void setNameSpacePath(String nameSpacePath) {
        this.nameSpacePath = nameSpacePath;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public List<Pair<Long, Double>> getQuerySeries() {
        return querySeries;
    }

    public void setQuerySeries(List<Pair<Long, Double>> querySeries) {
        this.querySeries = querySeries;
    }
}
