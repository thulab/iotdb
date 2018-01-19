package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperationReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;

/**
 * This class is used to combine one series with corresponding update operations
 * Created by zhangjinrui on 2018/1/15.
 */
public class SeriesWithUpdateOpReader implements SeriesReader {

    private TimeValuePairReader seriesReader;
    private OverflowUpdateOperationReader overflowUpdateOperationReader;
    private OverflowUpdateOperation overflowUpdateOperation;
    private boolean hasOverflowUpdateOperation;

    public SeriesWithUpdateOpReader(TimeValuePairReader seriesReader,
                                    OverflowUpdateOperationReader overflowUpdateOperationReader) throws IOException {
        this.seriesReader = seriesReader;
        this.overflowUpdateOperationReader = overflowUpdateOperationReader;
        if (overflowUpdateOperationReader.hasNext()) {
            overflowUpdateOperation = overflowUpdateOperationReader.next();
            hasOverflowUpdateOperation = true;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        TimeValuePair timeValuePair = seriesReader.next();
        if (hasOverflowUpdateOperation) {
            long timestamp = timeValuePair.getTimestamp();
            while (overflowUpdateOperation.getRightBound() < timestamp) {
                if (overflowUpdateOperationReader.hasNext()) {
                    overflowUpdateOperation = overflowUpdateOperationReader.next();
                    hasOverflowUpdateOperation = true;
                } else {
                    hasOverflowUpdateOperation = false;
                    break;
                }
            }
            if (hasOverflowUpdateOperation &&
                    overflowUpdateOperation.getLeftBound() <= timestamp && timestamp <= overflowUpdateOperation.getRightBound()) {
                timeValuePair.setValue(overflowUpdateOperation.getValue());
            }
        }
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }
}
