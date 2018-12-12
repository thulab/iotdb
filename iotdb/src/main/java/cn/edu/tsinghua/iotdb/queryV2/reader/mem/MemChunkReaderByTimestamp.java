package cn.edu.tsinghua.iotdb.queryV2.reader.mem;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.queryV2.reader.merge.EngineSeriesReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.Iterator;

public class MemChunkReaderByTimestamp implements EngineSeriesReaderByTimeStamp {
    private Iterator<TimeValuePair> timeValuePairIterator;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    public MemChunkReaderByTimestamp(RawSeriesChunk rawSeriesChunk) {
        timeValuePairIterator = rawSeriesChunk.getIterator();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedTimeValuePair) {
            return true;
        }
        return timeValuePairIterator.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasCachedTimeValuePair) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        } else {
            return timeValuePairIterator.next();
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {

    }

    //TODO 可以考虑将成员变量timeValuePairIterator更改为list形式，然后将顺序查找改为二分查找
    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        while(hasNext()){
            TimeValuePair timeValuePair = next();
            long time = timeValuePair.getTimestamp();
            if(time == timestamp){
                return timeValuePair.getValue();
            }
            else if(time > timestamp){
                hasCachedTimeValuePair = true;
                cachedTimeValuePair = timeValuePair;
                break;
            }
        }
        return null;
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
