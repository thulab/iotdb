package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.common.Path;


public interface OverflowQuerier {

    /**
     * <p>
     * Get a series reader for given path. Inserted Data for this series in overflow
     * could be retrieved by this reader
     *
     * @param path   aimed series's path
     * @param filter filter for
     * @return
     */
    OverflowSeriesReader getOverflowInsertDataSeriesReader(Path path, Filter<?> filter);

    /**
     * Get the OverflowOperationReader for corresponding series.
     * @param path
     * @param filter
     * @return
     */
    OverflowOperationReader getOverflowUpdateOperationReader(Path path, Filter<?> filter);

}
