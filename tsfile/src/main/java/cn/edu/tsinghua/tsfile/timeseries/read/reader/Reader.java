package cn.edu.tsinghua.tsfile.timeseries.read.reader;

import java.io.IOException;

public interface Reader {

    /**
     * whether this page has data
     */
    boolean hasNextBatch() throws IOException;

    /**
     * get next batch data
     */
    BatchData nextBatch();


    void close() throws IOException;
}

