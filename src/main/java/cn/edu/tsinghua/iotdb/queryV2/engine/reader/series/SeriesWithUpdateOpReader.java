package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperationReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

/**
 * This class is used to combine one series with corresponding update operations
 * Created by zhangjinrui on 2018/1/15.
 */
public class SeriesWithUpdateOpReader implements SeriesReader {

    private SeriesReader seriesReader;
    private OverflowUpdateOperationReader overflowUpdateOperationReader;

    public SeriesWithUpdateOpReader(SeriesReader seriesReader,
                                    OverflowUpdateOperationReader overflowUpdateOperationReader) throws IOException {
        this.seriesReader = seriesReader;
        this.overflowUpdateOperationReader = overflowUpdateOperationReader;
    }

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
}
