package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;

/**
 * <p> The bigger the priority value is, the higher the priority is.
 */
public class PriorityTimeValuePairReader implements ISeriesReader {

    protected ISeriesReader seriesReader;
    protected Priority priority;

    public PriorityTimeValuePairReader(ISeriesReader seriesReader, Priority priority) {
        this.seriesReader = seriesReader;
        this.priority = priority;
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        return seriesReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        seriesReader.next();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }

    public Priority getPriority() {
        return priority;
    }

    public static class Priority implements Comparable<Priority> {

        private int priority;

        public Priority(int priority) {
            this.priority = priority;
        }

        @Override
        public int compareTo(Priority o) {
            return this.priority - o.priority;
        }
    }

    // TODO
    @Override public boolean hasNextBatch() {
        return false;
    }

    // TODO
    @Override public BatchData nextBatch() {
        return null;
    }

    // TODO
    @Override public BatchData currentBatch() {
        return null;
    }
}
