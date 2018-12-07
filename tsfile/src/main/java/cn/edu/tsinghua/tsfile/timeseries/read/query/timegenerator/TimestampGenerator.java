package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;

import java.io.IOException;

/**
 * All SeriesFilter involved in a QueryFilter will be transferred to a TimeGenerator tree whose leaf nodes are all SeriesReaders,
 * The TimeGenerator tree can generate the next timestamp that satisfies the filter condition.
 *
 * Then we use this timestamp to get values in other series that are not included in QueryFilter
 */
public interface TimestampGenerator {

    boolean hasNext() throws IOException;

    long next() throws IOException;

    Object getValue(Path path, long time) throws IOException;

}
