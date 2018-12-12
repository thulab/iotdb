package cn.edu.tsinghua.iotdb.queryV2.engine.reader.unsequence;

import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.iotdb.read.Utils;
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
        TimeValuePair timeValuePair = Utils.getCurrenTimeValuePair(data);
        data.next();
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
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
