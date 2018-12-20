package cn.edu.tsinghua.iotdb.query.reader.unsequence;

import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePairUtils;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;

public class EngineChunkReader implements IReader {

    protected ChunkReader reader;
    private BatchData data;

    public EngineChunkReader(ChunkReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (data == null || !data.hasNext()) {
            if (reader.hasNextBatch())
                data = reader.nextBatch();
            else
                return false;
        }

        return data.hasNext();
    }

    @Override
    public TimeValuePair next() {
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
        data.next();
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() {
        next();
    }

    @Override
    public void close() throws IOException {
        reader.close();
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
