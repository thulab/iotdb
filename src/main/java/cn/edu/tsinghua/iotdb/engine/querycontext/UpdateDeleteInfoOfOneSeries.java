package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReaderImpl;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class UpdateDeleteInfoOfOneSeries {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDeleteInfoOfOneSeries.class);
    private TSDataType dataType;
    private List<OverflowUpdateDeleteFile> overflowUpdateFileList;
    private DynamicOneColumnData overflowUpdateInMem;
    private OverflowOperationReader overflowUpdateOperationReader;

    public UpdateDeleteInfoOfOneSeries(TSDataType dataType, List<OverflowUpdateDeleteFile> overflowUpdateFileList, DynamicOneColumnData overflowUpdateInMem) {
        this.dataType = dataType;
        this.overflowUpdateFileList = overflowUpdateFileList;
        this.overflowUpdateInMem = overflowUpdateInMem;
    }

    public OverflowOperationReader getOverflowUpdateOperationReader() {
        if (overflowUpdateOperationReader == null) {
            overflowUpdateOperationReader = new OverflowOperationReaderImpl(overflowUpdateInMem, overflowUpdateFileList, dataType);
        }

        return overflowUpdateOperationReader;
    }
}
