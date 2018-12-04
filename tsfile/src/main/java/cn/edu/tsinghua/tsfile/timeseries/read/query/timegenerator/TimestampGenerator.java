package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import java.io.IOException;


public interface TimestampGenerator {

    boolean hasNext() throws IOException;

    long next() throws IOException;

}
