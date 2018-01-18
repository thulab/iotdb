package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperationReader;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class UpdateDeleteInfoOfOneSeries {
    private OverflowUpdateOperationReader overflowUpdateOperationReader;
    private Filter<Long> deleteFilter;

    public OverflowUpdateOperationReader getOverflowUpdateOperationReader() {
        return overflowUpdateOperationReader;
    }

    public void setOverflowUpdateOperationReader(OverflowUpdateOperationReader overflowUpdateOperationReader) {
        this.overflowUpdateOperationReader = overflowUpdateOperationReader;
    }

    public Filter<Long> getDeleteFilter() {
        return deleteFilter;
    }

    public void setDeleteFilter(Filter<Long> deleteFilter) {
        this.deleteFilter = deleteFilter;
    }
}
