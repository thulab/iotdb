package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.List;


public class LinearFill extends IFill{

    private long beforeRange, afterRange;

    private Path path;

    public LinearFill(long beforeRange, long afterRange) {
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
    }

    public LinearFill(TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
        super(dataType, queryTime);
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
    }

    public LinearFill(Path path, TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
        super(dataType, queryTime);
        this.path = path;
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
    }

    @Override
    public IFill copy(Path path) {
        return new LinearFill(path, getDataType(), getQueryTime(), beforeRange, afterRange);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public DynamicOneColumnData getFillResult() {
        return null;
    }
}
