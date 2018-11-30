package cn.edu.tsinghua.iotdb.qp.logical.crud;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.query.fill.IFill;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.List;
import java.util.Map;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 * 
 */
public class QueryOperator extends SFWOperator {

    public QueryOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = Operator.OperatorType.QUERY;
    }

    private long unit;
    private long origin;
    private List<Pair<Long, Long>> intervals;
    private boolean isGroupBy = false;

    private Map<TSDataType, IFill> fillTypes;
    private boolean isFill = false;

    private int seriesLimit;
    private int seriesOffset;
    private boolean isSlimit = false; // false if sql does not contain SLIMIT clause

    public boolean isFill() {
        return isFill;
    }

    public void setFill(boolean fill) {
        isFill = fill;
    }

    public Map<TSDataType, IFill> getFillTypes() {
        return fillTypes;
    }

    public void setFillTypes(Map<TSDataType, IFill> fillTypes) {
        this.fillTypes = fillTypes;
    }

    public void setGroupBy(boolean isGroupBy) {
        this.isGroupBy = isGroupBy;
    }

    public boolean isGroupBy() {
        return isGroupBy;
    }

    public void setSlimit(int seriesLimit) {
        this.seriesLimit = seriesLimit;
        this.isSlimit = true;
    }

    public void setSoffset(int seriesOffset) {
        /*
         Since soffset cannot be set alone without slimit,
         `isSlimit` only need to be set true in the `setSlimit` function.
         */
        this.seriesOffset = seriesOffset;
    }

    public int getSeriesLimit() {
        return seriesLimit;
    }

    public int getSeriesOffset() {
        return seriesOffset;
    }

    public boolean isSlimit() {
        return isSlimit;
    }

    public long getUnit() {
        return unit;
    }

    public void setUnit(long unit) {
        this.unit = unit;
    }

    public void setOrigin(long origin) {
        this.origin = origin;
    }

    public long getOrigin() {
        return origin;
    }

    public void setIntervals(List<Pair<Long, Long>> intervals) {
        this.intervals = intervals;
    }

    public List<Pair<Long, Long>> getIntervals() {
        return intervals;
    }

}
