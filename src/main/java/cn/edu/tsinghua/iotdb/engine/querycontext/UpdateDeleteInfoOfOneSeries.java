package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class UpdateDeleteInfoOfOneSeries {
    private TSDataType dataType;
    private List<OverflowUpdateDeleteFile> overflowUpdateFileList;
    private DynamicOneColumnData overflowUpdateInMem;

    public OverflowOperationReader getOverflowUpdateOperationReader() {
        //TODO: CGF
        return null;
    }

    public Filter<Long> getDeleteFilter() {
        return null;
    }
}
