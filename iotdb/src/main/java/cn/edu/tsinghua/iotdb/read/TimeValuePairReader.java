package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface TimeValuePairReader {

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;
}

