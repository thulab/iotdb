package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface IReader {

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;

    boolean hasNextBatch();

    BatchData nextBatch();

    BatchData currentBatch();
}

