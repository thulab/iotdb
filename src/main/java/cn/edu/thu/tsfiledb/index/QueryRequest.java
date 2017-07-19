package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;

import java.util.List;

/**
 * The abstract class for a query request with specific parameters.
 *
 * @author Jiaye Wu
 */
public abstract class QueryRequest {

    protected Path columnPath;

    protected long startTime;

    protected long endTime;

    protected List<Pair<Long, Double>> querySeries;

    protected QueryRequest(Path columnPath, long startTime, long endTime, List<Pair<Long, Double>> querySeries) {
        this.columnPath = columnPath;
        this.startTime = startTime;
        this.endTime = endTime;
        this.querySeries = querySeries;
    }

    protected QueryRequest(Path columnPath, List<Pair<Long, Double>> querySeries) {
        this.columnPath = columnPath;
        this.startTime = Long.MIN_VALUE;
        this.endTime = Long.MAX_VALUE;
        this.querySeries = querySeries;
    }

    public Path getColumnPath() {
        return columnPath;
    }

    public void setColumnPath(Path columnPath) {
        this.columnPath = columnPath;
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
