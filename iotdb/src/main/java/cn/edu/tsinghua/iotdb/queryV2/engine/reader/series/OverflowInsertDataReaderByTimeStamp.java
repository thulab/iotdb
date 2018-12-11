package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.SeriesReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;

public class OverflowInsertDataReaderByTimeStamp implements SeriesReaderByTimeStamp {

    private Long jobId;
    private PriorityMergeReaderByTimestamp seriesReader;

    public OverflowInsertDataReaderByTimeStamp(Long jobId, PriorityMergeReaderByTimestamp seriesReader){
        this.jobId = jobId;
        this.seriesReader = seriesReader;
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
        seriesReader.skipCurrentTimeValuePair();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        return seriesReader.getValueInTimestamp(timestamp);
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
