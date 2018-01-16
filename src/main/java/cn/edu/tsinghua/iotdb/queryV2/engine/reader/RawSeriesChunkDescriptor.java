package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;

/**
 * Created by zhangjinrui on 2018/1/15.
 */
public class RawSeriesChunkDescriptor implements SeriesChunkDescriptor {

    @Override
    public TSDataType getDataType() {
        return null;
    }

    @Override
    public TsDigest getValueDigest() {
        return null;
    }

    @Override
    public long getMinTimestamp() {
        return 0;
    }

    @Override
    public long getMaxTimestamp() {
        return 0;
    }

    @Override
    public long getCountOfPoints() {
        return 0;
    }
}
