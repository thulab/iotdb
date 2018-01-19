package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * used for overflow update operation
 */
public class UpdateOperation {
    private DynamicOneColumnData updateOperation;
    private int idx;
    private SingleSeriesFilterExpression valueFilter;
    private SingleValueVisitor singleValueVisitor;
    private TSDataType dataType;

    public UpdateOperation(TSDataType dataType, DynamicOneColumnData data) {
        this.dataType = dataType;

        if (data == null) {
            this.updateOperation = new DynamicOneColumnData(dataType, true);
        } else {
            this.updateOperation = data;
        }
        idx = 0;
    }

    public UpdateOperation(TSDataType dataType, DynamicOneColumnData data, SingleSeriesFilterExpression valueFilter) {
        this.dataType = dataType;

        if (data == null) {
            this.updateOperation = new DynamicOneColumnData(dataType, true);
        } else {
            this.updateOperation = data;
        }
        idx = 0;
        this.valueFilter = valueFilter;
        //this.singleValueVisitor = ReaderUtils.getSingleValueVisitorByDataType(dataType, valueFilter);
        this.singleValueVisitor = new SingleValueVisitor();
    }

    public boolean hasNext() {
        return idx < updateOperation.valueLength;
    }

    public long getUpdateStartTime() {
        return updateOperation.getTime(idx * 2);
    }

    public long getUpdateEndTime() {
        return updateOperation.getTime(idx * 2 + 1);
    }

    public void next() {
        idx ++;
    }

    public int getInt() {
        return updateOperation.getInt(idx);
    }

    public long getLong() {
        return updateOperation.getLong(idx);
    }

    public float getFloat() {
        return updateOperation.getFloat(idx);
    }

    public double getDouble() {
        return updateOperation.getDouble(idx);
    }

    public boolean getBoolean() {
        return updateOperation.getBoolean(idx);
    }

    public Binary getText() {
        return updateOperation.getBinary(idx);
    }

    public boolean verifyTimeValue(long time) {
        return verifyTime(time) && verifyValue();
    }

    public boolean verifyTime(long time) {
        if (idx < updateOperation.valueLength && getUpdateStartTime() <= time && getUpdateEndTime() >= time)
            return true;

        return false;
    }

    public boolean verifyValue() {
        assert idx < updateOperation.valueLength;

        if (valueFilter == null)
            return true;

        switch (dataType) {
            case INT32:
                return singleValueVisitor.satisfyObject(getInt(), valueFilter);
            case INT64:
                return singleValueVisitor.satisfyObject(getLong(), valueFilter);
            case FLOAT:
                return singleValueVisitor.satisfyObject(getFloat(), valueFilter);
            case DOUBLE:
                return singleValueVisitor.satisfyObject(getDouble(), valueFilter);
            case BOOLEAN:
                return singleValueVisitor.satisfyObject(getBoolean(), valueFilter);
            case TEXT:
                return singleValueVisitor.satisfyObject(getText(), valueFilter);
        }

        return false;
    }

    public boolean verifyTimeDigest(long minTime, long maxTime) {
        return false;
    }
}
