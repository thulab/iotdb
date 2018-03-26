package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to combine one series with corresponding update operations
 * Created by zhangjinrui on 2018/1/15.
 */
public class SeriesWithUpdateOpReader implements SeriesReader {

    private PriorityMergeSortTimeValuePairReader seriesReader;
    private OverflowOperationReader overflowOperationReader;
    private OverflowOperation overflowUpdateOperation;
    private boolean hasOverflowUpdateOperation;

    private List<FileNodeProcessor> fileNodeProcessorList;

    public SeriesWithUpdateOpReader(TimeValuePairReader seriesReader,
                                    OverflowOperationReader overflowOperationReader) throws IOException {
        this.seriesReader = (PriorityMergeSortTimeValuePairReader)seriesReader;
        this.overflowOperationReader = overflowOperationReader;
        if (overflowOperationReader.hasNext()) {
            overflowUpdateOperation = overflowOperationReader.next();
            hasOverflowUpdateOperation = true;
        }

        fileNodeProcessorList = new ArrayList<>();
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
                if (overflowOperationReader.hasNext()) {
                    overflowUpdateOperation = overflowOperationReader.next();
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

    public long getPointCoverNum(){
        if(seriesReader == null)return 0;
        else return seriesReader.getPointCoverNum();
    }
}
