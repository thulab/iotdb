package cn.edu.tsinghua.iotdb.query.fill;


import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.engine.EngineUtils;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.*;

import java.io.IOException;
import java.util.List;

public class PreviousFill extends IFill {

    private long beforeRange;

    private Path path;

    private DynamicOneColumnData result;

    public PreviousFill(TSDataType dataType, long queryTime, long beforeRange) {
        super(dataType, queryTime);
        this.beforeRange = beforeRange;
        result = new DynamicOneColumnData(dataType, true, true);
    }

    public PreviousFill(Path path, TSDataType dataType, long queryTime, long beforeRange) {
        super(dataType, queryTime);
        this.path = path;
        this.beforeRange = beforeRange;
        result = new DynamicOneColumnData(dataType, true, true);
    }

    public PreviousFill(long beforeRange) {
        this.beforeRange = beforeRange;
    }

    @Override
    public IFill copy(Path path) {
        return new PreviousFill(path, dataType, queryTime, beforeRange);
    }

    public long getBeforeRange() {
        return beforeRange;
    }


    @Override
    public DynamicOneColumnData getFillResult() throws ProcessorException, IOException, PathErrorException {

        SingleSeriesFilterExpression leftFilter = gtEq(timeFilterSeries(), queryTime - beforeRange, true);
        SingleSeriesFilterExpression rightFilter = ltEq(timeFilterSeries(), queryTime, true);
        SingleSeriesFilterExpression fillTimeFilter = (SingleSeriesFilterExpression) and(leftFilter, rightFilter);

        String deltaObjectId = path.getDeltaObjectToString();
        String measurementId = path.getMeasurementToString();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix("FillQuery", -1);

        RecordReader recordReader = RecordReaderFactory.getInstance().getRecordReader(deltaObjectId, measurementId,
                fillTimeFilter, null, null, null, recordReaderPrefix);

        List<Object> params = EngineUtils.getOverflowInfoAndFilterDataInMem(fillTimeFilter, null, null,
                null, recordReader.insertPageInMemory, recordReader.overflowInfo);

        DynamicOneColumnData insertTrue = (DynamicOneColumnData) params.get(0);
        DynamicOneColumnData updateTrue = (DynamicOneColumnData) params.get(1);
        DynamicOneColumnData updateFalse = (DynamicOneColumnData) params.get(2);
        SingleSeriesFilterExpression overflowTimeFilter = (SingleSeriesFilterExpression) params.get(3);

        recordReader.insertAllData = new InsertDynamicData(recordReader.bufferWritePageList, recordReader.compressionTypeName,
                insertTrue, updateTrue, updateFalse,
                overflowTimeFilter, null, null, dataType);

        recordReader.getPreviousFillResult(result, deltaObjectId, measurementId,
                updateTrue, updateFalse, recordReader.insertAllData, overflowTimeFilter, queryTime - beforeRange, queryTime);

        return result;
    }
}