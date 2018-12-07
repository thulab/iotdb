package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.management.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.reader.FillRecordReader;
import cn.edu.tsinghua.iotdb.query.reader.ReaderType;
import cn.edu.tsinghua.iotdb.query.reader.RecordReaderFactory;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;

import java.io.IOException;


public class LinearFill extends IFill{

    private long beforeRange, afterRange;

    private Path path;

    private BatchData result;

    public LinearFill(long beforeRange, long afterRange) {
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
    }

    public LinearFill(Path path, TSDataType dataType, long queryTime, long beforeRange, long afterRange) {
        super(dataType, queryTime);
        this.path = path;
        this.beforeRange = beforeRange;
        this.afterRange = afterRange;
        result = new BatchData(dataType, true, true);
    }

    public long getBeforeRange() {
        return beforeRange;
    }

    public void setBeforeRange(long beforeRange) {
        this.beforeRange = beforeRange;
    }

    public long getAfterRange() {
        return afterRange;
    }

    public void setAfterRange(long afterRange) {
        this.afterRange = afterRange;
    }

    @Override
    public IFill copy(Path path) {
        return new LinearFill(path, dataType, queryTime, beforeRange, afterRange);
    }

    @Override
    public BatchData getFillResult() throws ProcessorException, IOException, PathErrorException {
        long beforeTime, afterTime;
        if (beforeRange == -1) {
            beforeTime = 0;
        } else {
            beforeTime = queryTime - beforeRange;
        }
        if (afterRange == -1) {
            afterTime = Long.MAX_VALUE;
        } else {
            afterTime = queryTime + afterRange;
        }

        Filter leftFilter = TimeFilter.gtEq(beforeTime);
        Filter rightFilter = TimeFilter.ltEq(afterTime);
        GlobalTimeFilter fillTimeFilter = new GlobalTimeFilter(FilterFactory.and(leftFilter, rightFilter));

        String deltaObjectId = path.getDevice();
        String measurementId = path.getMeasurement();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix("LinearFill", -1);

        FillRecordReader recordReader = (FillRecordReader) RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                fillTimeFilter, null, null, recordReaderPrefix, ReaderType.FILL);

        recordReader.getLinearFillResult(result, fillTimeFilter, beforeTime, queryTime, afterTime);

        return result;
    }
}
