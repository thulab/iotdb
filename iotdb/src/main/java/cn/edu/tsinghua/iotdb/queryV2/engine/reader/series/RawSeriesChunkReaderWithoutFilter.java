package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.Iterator;

public class RawSeriesChunkReaderWithoutFilter implements ISeriesReader {

    private Iterator<TimeValuePair> timeValuePairIterator;

    public RawSeriesChunkReaderWithoutFilter(RawSeriesChunk rawSeriesChunk) {
        timeValuePairIterator = rawSeriesChunk.getIterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeValuePairIterator.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        return timeValuePairIterator.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNextBatch() {
        return false;
    }

    @Override
    public BatchData nextBatch() {
        return null;
    }

    @Override
    public BatchData currentBatch() {
        return null;
    }
}
