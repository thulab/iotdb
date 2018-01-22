package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public interface OverflowOperationReader {

    boolean hasNext();

    OverflowOperation next();

    void close() throws IOException;
}
