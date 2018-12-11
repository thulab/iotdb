package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;

import java.io.IOException;

public class PrioritySeriesReaderByTimestamp extends PrioritySeriesReader
        implements SeriesReaderByTimeStamp, Comparable<PrioritySeriesReaderByTimestamp>  {

    public PrioritySeriesReaderByTimestamp(SeriesReaderByTimeStamp seriesReader, Priority priority){
        super(seriesReader, priority);
    }

    @Override
    public int compareTo(PrioritySeriesReaderByTimestamp o) {
        return this.priority.compareTo(o.getPriority());
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        return ((SeriesReaderByTimeStamp)seriesReader).getValueInTimestamp(timestamp);
    }
}
