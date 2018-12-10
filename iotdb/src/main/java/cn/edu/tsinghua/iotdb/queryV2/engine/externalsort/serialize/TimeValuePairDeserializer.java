package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;


public interface TimeValuePairDeserializer extends TimeValuePairReader{

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    /**
     * Close current deserializer
     * @throws IOException
     */
    void close() throws IOException;
}
