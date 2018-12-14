package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.Iterator;

public interface RawSeriesChunk {

    TSDataType getDataType();

    long getMaxTimestamp();

    long getMinTimestamp();

    TsPrimitiveType getValueAtMaxTime();

    TsPrimitiveType getValueAtMinTime();

    Iterator<TimeValuePair> getIterator();
    
    boolean isEmpty();
}
