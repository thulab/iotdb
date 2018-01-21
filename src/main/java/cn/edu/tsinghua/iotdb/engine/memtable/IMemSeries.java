package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

/**
 *
 * @author Rong Kang
 */
public interface IMemSeries {

    void putLong(long t, long v);

    void putInt(long t, int v);

    void putFloat(long t, float v);

    void putDouble(long t, double v);

    void putBinary(long t, Binary v);

    void putBoolean(long t, boolean v);

    void write(TSDataType dataType, long insertTime, String insertValue);

    void sortAndDeduplicate();

    Iterable<TimeValuePair> query();

    void reset();

    int size();
}
