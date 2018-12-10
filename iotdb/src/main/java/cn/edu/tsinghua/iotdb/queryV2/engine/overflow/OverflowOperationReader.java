package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;

import java.io.IOException;


public interface OverflowOperationReader {

    boolean hasNext();

    /**
     * notice that : invoking this method will remove current overflow operation.
     */
    OverflowOperation next();

    /**
     * notice that : invoking this method will not remove current overflow operation.
     */
    OverflowOperation getCurrentOperation();

    void close() throws IOException;

    OverflowOperationReader copy();
}
