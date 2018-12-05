package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import java.io.IOException;

/**
 * All SeriesFilter involved in QueryFilter will be transferred to a TimeGenerator tree whose leaf node is a SeriesReader,
 * This TimeGenerator tree will generate next timestamp that satisfies the filter condition.
 *
 * Then we use this timestamp to get values in other series that do not included in QueryFilter
 */
public interface TimestampGenerator {

    boolean hasNext() throws IOException;

    long next() throws IOException;

}
