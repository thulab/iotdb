package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;

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
