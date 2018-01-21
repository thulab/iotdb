package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

import java.util.Iterator;

/**
 * Created by zhangjinrui on 2018/1/21.
 */
public class SeriesInMemTable implements RawSeriesChunk {

    @Override
    public TSDataType getDataType() {
        return null;
    }

    @Override
    public long getMaxTimestamp() {
        return 0;
    }

    @Override
    public long getMinTimestamp() {
        return 0;
    }

    @Override
    public TsPrimitiveType getMaxValue() {
        return null;
    }

    @Override
    public TsPrimitiveType getMinValue() {
        return null;
    }

    @Override
    public Iterator<TimeValuePair> getIterator() {
        return null;
    }
}
