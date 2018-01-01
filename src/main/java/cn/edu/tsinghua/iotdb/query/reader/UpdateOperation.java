package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * Created by beyyes on 17/12/31.
 */
public class UpdateOperation {
    private DynamicOneColumnData updateTrue;
    private DynamicOneColumnData updateFalse;

    public UpdateOperation() {}

    public long getUpdateStartTime() {
        return 0;
    }
}
