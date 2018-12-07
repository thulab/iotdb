package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.tsfile.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.reader.SeriesReader;

import java.io.IOException;

/**
 * Read TimeValuePair for one Series in BufferWrite which concludes all seriesChunks in TsFile and MemTable.
 */
public class SequentialSeriesReaderImpl implements SeriesReader {
    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return null;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
