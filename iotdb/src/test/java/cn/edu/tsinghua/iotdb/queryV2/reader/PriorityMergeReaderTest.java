package cn.edu.tsinghua.iotdb.queryV2.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PrioritySeriesReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PriorityMergeReaderTest {
    @Test
    public void test() throws IOException {
        FakedPrioritySeriesReader reader1 = new FakedPrioritySeriesReader(100, 20, 5, 11, 1);
        FakedPrioritySeriesReader reader2 = new FakedPrioritySeriesReader(150, 20, 5, 19, 2);
        FakedPrioritySeriesReader reader3 = new FakedPrioritySeriesReader(180, 20, 5, 31, 3);

        PriorityMergeReader priorityMergeReader = new PriorityMergeReader(reader1, reader2, reader3);
        int cnt = 0;
        while (priorityMergeReader.hasNext()){
            TimeValuePair timeValuePair = priorityMergeReader.next();
            long time = timeValuePair.getTimestamp();
            long value = timeValuePair.getValue().getLong();
            if(time < 150){
                Assert.assertEquals(time % 11, value);
            }
            else if(time < 180){
                Assert.assertEquals(time % 19, value);
            }
            else {
                Assert.assertEquals(time % 31, value);
            }
            cnt++;
        }
        Assert.assertEquals(180/5, cnt);
    }

    public static class FakedPrioritySeriesReader extends PrioritySeriesReader {

        public FakedPrioritySeriesReader(TimeValuePairReader seriesReader, Priority priority) {
            super(seriesReader, priority);
        }

        public FakedPrioritySeriesReader(long startTime, int size, int interval, int modValue, int priority) {
            this(new FakedTimeValuePairReader(startTime, size, interval, modValue), new Priority(priority));
        }
    }

    public static class FakedTimeValuePairReader implements TimeValuePairReader {
        private Iterator<TimeValuePair> iterator;


        public FakedTimeValuePairReader(long startTime, int size, int interval, int modValue){
            long time = startTime;
            List<TimeValuePair>  list = new ArrayList<>();
            for(int i = 0; i < size; i++){
                list.add(new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue)));
                time+=interval;
            }
            iterator = list.iterator();
        }

        @Override
        public boolean hasNext() throws IOException {
            return iterator.hasNext();
        }

        @Override
        public TimeValuePair next() throws IOException {
            return iterator.next();
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            iterator.next();
        }

        @Override
        public void close() throws IOException {

        }
    }
}
