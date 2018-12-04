package cn.edu.tsinghua.tsfile.timeseries.read.reader;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.IOException;

public interface Reader {

    /**
     * if there is a next time-value pair
     */
    boolean hasNext() throws IOException;

    /**
     * @return next time value pair
     */
    TimeValuePair next() throws IOException;

    /**
     * skip the current time value pair, just call next()
     */
    void skipCurrentTimeValuePair() throws IOException;


    /**
     * whether this page has data
     */
    boolean hasNextBatch() throws IOException;

    /**
     * get next batch data
     */
    DynamicOneColumnData nextBatch();


    void close() throws IOException;
}

