package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.tsfile.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.reader.SeriesReader;

import java.io.IOException;

/**
 * Read TimeValuePair in one SeriesChunkGroup.
 * This reader may use external sort to reduce the usage of memory
 */
public class SeriesInOneGroupReaderImpl implements SeriesReader {
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
