package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * Created by beyyes on 17/12/31.
 */
public class UpdateOperation {
    private DynamicOneColumnData updateOperation;
    private int idx;

    public UpdateOperation(DynamicOneColumnData data) {
        this.updateOperation = data;
        idx = 0;
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
}
