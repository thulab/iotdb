package cn.edu.tsinghua.iotdb.query.v2;

import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class QueryRecordReader extends RecordReader {

    private static final Logger logger = LoggerFactory.getLogger(QueryRecordReader.class);
    
    private OverflowOperationReader overflowOperationReaderForFile;
    
    public QueryRecordReader(GlobalSortedSeriesDataSource globalSortedSeriesDataSource, OverflowSeriesDataSource overflowSeriesDataSource,
                             String deltaObjectId, String measurementId,
                             SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter)
            throws PathErrorException, IOException {
        super(globalSortedSeriesDataSource, overflowSeriesDataSource, deltaObjectId, measurementId, queryTimeFilter, queryValueFilter);

        overflowOperationReaderForFile = overflowOperationReader.copy();
    }

    private int usedRowGroupReaderIndex;
    private int usedValueReaderIndex;

    /**
     * Query the data of one given series.
     */
    public DynamicOneColumnData queryOneSeries(SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter,
                                               DynamicOneColumnData res, int fetchSize) throws IOException {

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, queryTimeFilter);
        int rowGroupIndex = 0;
        if (res != null) {
            rowGroupIndex = res.getRowGroupIndex();
        }

        while (rowGroupIndex < rowGroupReaderList.size()) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                res = queryOneSeries(rowGroupReader.getValueReaders().get(measurementId), queryTimeFilter, queryValueFilter, res, fetchSize);
                if (res.valueLength >= fetchSize) {
                    return res;
                }
            }
            rowGroupIndex ++;
        }

        while (usedValueReaderIndex < valueReaders.size()) {
            if (valueReaders.get(usedValueReaderIndex).getDataType().equals(dataType)) {
                res = queryOneSeries(valueReaders.get(usedValueReaderIndex), queryTimeFilter, queryValueFilter, res, fetchSize);
                if (res.valueLength >= fetchSize) {
                    return res;
                }
            }
            usedValueReaderIndex ++;
        }

        res = res == null ? new DynamicOneColumnData(dataType, true) : res;

        while (insertMemoryData.hasNext()) {
            putMemoryDataToResult(res, insertMemoryData);
            insertMemoryData.removeCurrentValue();

            if (res.valueLength >= fetchSize) {
                return res;
            }
        }

        return res;
    }

    private DynamicOneColumnData queryOneSeries(ValueReader valueReader, SingleSeriesFilterExpression timeFilter,
                                                SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize)
            throws IOException {

        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        if (res == null) {
            res = new DynamicOneColumnData(dataType, true);
            res.pageOffset = valueReader.getFileOffset();
        }

        // new series read
        if (res.pageOffset == -1) {
            res.pageOffset = valueReader.getFileOffset();
        }

        TsDigest digest = valueReader.getDigest();
        DigestForFilter valueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        logger.debug(String.format("read one series digest normally, time range is [%s,%s], value range is [%s,%s]",
                valueReader.getStartTime(), valueReader.getEndTime(), valueDigest.getMinValue(), valueDigest.getMaxValue()));
        DigestVisitor valueDigestVisitor = new DigestVisitor();

        while (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().getRightBound() < valueReader.getStartTime()) {
            overflowOperationReaderForFile.next();
        }

        // skip the current series chunk according to time filter
        IntervalTimeVisitor seriesTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            logger.debug("series time digest does not satisfy time filter");
            res.plusRowGroupIndexAndInitPageOffset();
            return res;
        }

        // skip the current series chunk according to value filter
        if (valueFilter != null && !valueDigestVisitor.satisfy(valueDigest, valueFilter)) {
            if ((!overflowOperationReaderForFile.hasNext() || overflowOperationReaderForFile.getCurrentOperation().getLeftBound() > valueReader.getEndTime()) &&
                    (!insertMemoryData.hasNext() || insertMemoryData.getCurrentMinTime() > valueReader.getEndTime())) {
                logger.debug("series value digest does not satisfy value filter");
                res.plusRowGroupIndexAndInitPageOffset();
                return res;
            }
        }

        // initial one page from file
        ByteArrayInputStream bis = valueReader.initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, compressionTypeName);
        int resCount = res.valueLength - res.curIdx;

        while ((res.pageOffset - valueReader.fileOffset) < valueReader.totalSize && resCount < fetchSize) {
            // to help to record byte size in this process of read.
            int lastAvailable = bis.available();
            PageHeader pageHeader = pageReader.getNextPageHeader();

            // construct value digest
            DigestForFilter pageValueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            while (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().getRightBound() < pageMinTime) {
                overflowOperationReaderForFile.next();
            }

            // skip the current page according to time filter
            if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, pageMinTime, pageMaxTime)) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // skip the current page according to value filter
            if (valueFilter != null && !valueDigestVisitor.satisfy(pageValueDigest, valueFilter)) {
                if ((!overflowOperationReaderForFile.hasNext() || overflowOperationReaderForFile.getCurrentOperation().getLeftBound() > pageMaxTime) &&
                        (!insertMemoryData.hasNext() || insertMemoryData.getCurrentMinTime() > pageMaxTime)) {
                    pageReader.skipCurrentPage();
                    res.pageOffset += lastAvailable - bis.available();
                    continue;
                }
            }

            InputStream page = pageReader.getNextPage();
            res.pageOffset += lastAvailable - bis.available();
            long[] pageTimestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));

            int timeIdx = 0;
            switch (dataType) {
                case INT32:
                    int[] pageIntValues = new int[pageTimestamps.length];
                    int cnt = 0;
                    while (valueReader.decoder.hasNext(page)) {
                        pageIntValues[cnt++] = valueReader.decoder.readInt(page);
                    }

                    // TODO there may return many results
                    for (; timeIdx < pageTimestamps.length; timeIdx++) {
                        while (insertMemoryData.hasNext() && timeIdx < pageTimestamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putInt(insertMemoryData.getCurrentIntValue());
                            resCount++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }
                        if (timeIdx >= pageTimestamps.length)
                            break;

                        if (overflowOperationReaderForFile.hasNext()) {
                            if (overflowOperationReaderForFile.getCurrentOperation().verifyTime(pageTimestamps[timeIdx])) {
                                if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                                        || (valueFilter != null &&
                                        !singleValueVisitor.satisfyObject(overflowOperationReaderForFile.getCurrentOperation().getValue().getInt(), valueFilter))) {
                                    continue;
                                } else {
                                    res.putTime(pageTimestamps[timeIdx]);
                                    res.putInt(overflowOperationReaderForFile.getCurrentOperation().getValue().getInt());
                                    continue;
                                }
                            }
                        }

                        if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                (valueFilter == null || singleValueVisitor.verify(pageIntValues[timeIdx]))) {
                            res.putTime(pageTimestamps[timeIdx]);
                            res.putInt(pageIntValues[timeIdx]);
                            resCount++;
                        }
                    }
                    break;
                case BOOLEAN:
                    boolean[] pageBooleanValues = new boolean[pageTimestamps.length];
                    cnt = 0;
                    while (valueReader.decoder.hasNext(page)) {
                        pageBooleanValues[cnt++] = valueReader.decoder.readBoolean(page);
                    }

                    // TODO there may return many results
                    for (; timeIdx < pageTimestamps.length; timeIdx++) {
                        while (insertMemoryData.hasNext() && timeIdx < pageTimestamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putBoolean(insertMemoryData.getCurrentBooleanValue());
                            resCount++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }
                        if (timeIdx >= pageTimestamps.length)
                            break;

                        if (overflowOperationReaderForFile.hasNext()) {
                            if (overflowOperationReaderForFile.getCurrentOperation().verifyTime(pageTimestamps[timeIdx])) {
                                if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                                        || (valueFilter != null &&
                                        !singleValueVisitor.satisfyObject(overflowOperationReaderForFile.getCurrentOperation().getValue().getBoolean(), valueFilter))) {
                                    continue;
                                } else {
                                    res.putTime(pageTimestamps[timeIdx]);
                                    res.putBoolean(overflowOperationReaderForFile.getCurrentOperation().getValue().getBoolean());
                                    continue;
                                }
                            }
                        }

                        if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                (valueFilter == null || singleValueVisitor.satisfyObject(pageBooleanValues[timeIdx], valueFilter))) {
                            res.putTime(pageTimestamps[timeIdx]);
                            res.putBoolean(pageBooleanValues[timeIdx]);
                            resCount++;
                        }
                    }
                    break;
                case INT64:
                    long[] pageLongValues = new long[pageTimestamps.length];
                    cnt = 0;
                    while (valueReader.decoder.hasNext(page)) {
                        pageLongValues[cnt++] = valueReader.decoder.readLong(page);
                    }

                    // TODO there may return many results
                    for (; timeIdx < pageTimestamps.length; timeIdx++) {
                        while (insertMemoryData.hasNext() && timeIdx < pageTimestamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putLong(insertMemoryData.getCurrentLongValue());
                            resCount++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }
                        if (timeIdx >= pageTimestamps.length)
                            break;

                        if (overflowOperationReaderForFile.hasNext()) {
                            if (overflowOperationReaderForFile.getCurrentOperation().verifyTime(pageTimestamps[timeIdx])) {
                                if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                                        || (valueFilter != null &&
                                        !singleValueVisitor.satisfyObject(overflowOperationReaderForFile.getCurrentOperation().getValue().getLong(), valueFilter))) {
                                    continue;
                                } else {
                                    res.putTime(pageTimestamps[timeIdx]);
                                    res.putLong(overflowOperationReaderForFile.getCurrentOperation().getValue().getLong());
                                    continue;
                                }
                            }
                        }

                        if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                (valueFilter == null || singleValueVisitor.verify(pageLongValues[timeIdx]))) {
                            res.putTime(pageTimestamps[timeIdx]);
                            res.putLong(pageLongValues[timeIdx]);
                            resCount++;
                        }
                    }
                    break;
                case FLOAT:
                    float[] pageFloatValues = new float[pageTimestamps.length];
                    cnt = 0;
                    while (valueReader.decoder.hasNext(page)) {
                        pageFloatValues[cnt++] = valueReader.decoder.readFloat(page);
                    }

                    // TODO there may return many results
                    for (; timeIdx < pageTimestamps.length; timeIdx++) {
                        while (insertMemoryData.hasNext() && timeIdx < pageTimestamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putFloat(insertMemoryData.getCurrentFloatValue());
                            resCount++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }
                        if (timeIdx >= pageTimestamps.length)
                            break;

                        if (overflowOperationReaderForFile.hasNext()) {
                            if (overflowOperationReaderForFile.getCurrentOperation().verifyTime(pageTimestamps[timeIdx])) {
                                if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                                        || (valueFilter != null &&
                                        !singleValueVisitor.satisfyObject(overflowOperationReaderForFile.getCurrentOperation().getValue().getFloat(), valueFilter))) {
                                    continue;
                                } else {
                                    res.putTime(pageTimestamps[timeIdx]);
                                    res.putFloat(overflowOperationReaderForFile.getCurrentOperation().getValue().getFloat());
                                    continue;
                                }
                            }
                        }

                        if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                (valueFilter == null || singleValueVisitor.verify(pageFloatValues[timeIdx]))) {
                            res.putTime(pageTimestamps[timeIdx]);
                            res.putFloat(pageFloatValues[timeIdx]);
                            resCount++;
                        }
                    }
                    break;
                case DOUBLE:
                    double[] pageDoubleValues = new double[pageTimestamps.length];
                    cnt = 0;
                    while (valueReader.decoder.hasNext(page)) {
                        pageDoubleValues[cnt++] = valueReader.decoder.readDouble(page);
                    }

                    // TODO there may return many results
                    for (; timeIdx < pageTimestamps.length; timeIdx++) {
                        while (insertMemoryData.hasNext() && timeIdx < pageTimestamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putDouble(insertMemoryData.getCurrentDoubleValue());
                            resCount++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }
                        if (timeIdx >= pageTimestamps.length)
                            break;

                        if (overflowOperationReaderForFile.hasNext()) {
                            if (overflowOperationReaderForFile.getCurrentOperation().verifyTime(pageTimestamps[timeIdx])) {
                                if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                                        || (valueFilter != null &&
                                        !singleValueVisitor.satisfyObject(overflowOperationReaderForFile.getCurrentOperation().getValue().getDouble(), valueFilter))) {
                                    continue;
                                } else {
                                    res.putTime(pageTimestamps[timeIdx]);
                                    res.putDouble(overflowOperationReaderForFile.getCurrentOperation().getValue().getDouble());
                                    continue;
                                }
                            }
                        }

                        if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                (valueFilter == null || singleValueVisitor.verify(pageDoubleValues[timeIdx]))) {
                            res.putTime(pageTimestamps[timeIdx]);
                            res.putDouble(pageDoubleValues[timeIdx]);
                            resCount++;
                        }
                    }
                    break;
                case TEXT:
                    Binary[] pageBinaryValues = new Binary[pageTimestamps.length];
                    cnt = 0;
                    while (valueReader.decoder.hasNext(page)) {
                        pageBinaryValues[cnt++] = valueReader.decoder.readBinary(page);
                    }

                    // TODO there may return many results
                    for (; timeIdx < pageTimestamps.length; timeIdx++) {
                        while (insertMemoryData.hasNext() && timeIdx < pageTimestamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                            res.putTime(insertMemoryData.getCurrentMinTime());
                            res.putBinary(insertMemoryData.getCurrentBinaryValue());
                            resCount++;

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                insertMemoryData.removeCurrentValue();
                                timeIdx++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }
                        }
                        if (timeIdx >= pageTimestamps.length)
                            break;

                        if (overflowOperationReaderForFile.hasNext()) {
                            if (overflowOperationReaderForFile.getCurrentOperation().verifyTime(pageTimestamps[timeIdx])) {
                                if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE
                                        || (valueFilter != null &&
                                        !singleValueVisitor.satisfyObject(overflowOperationReaderForFile.getCurrentOperation().getValue().getBinary(), valueFilter))) {
                                    continue;
                                } else {
                                    res.putTime(pageTimestamps[timeIdx]);
                                    res.putBinary(overflowOperationReaderForFile.getCurrentOperation().getValue().getBinary());
                                    continue;
                                }
                            }
                        }

                        if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                (valueFilter == null || singleValueVisitor.satisfyObject(pageBinaryValues[timeIdx], valueFilter))) {
                            res.putTime(pageTimestamps[timeIdx]);
                            res.putBinary(pageBinaryValues[timeIdx]);
                            resCount++;
                        }
                    }
                    break;
                default:
                    throw new IOException("Data type not support. " + dataType);
            }
        }

        // represents that current series has been read all.
        if ((res.pageOffset - valueReader.fileOffset) >= valueReader.totalSize) {
            res.plusRowGroupIndexAndInitPageOffset();
        }

        return res;
    }

    /**
     *  <p> This function is used for cross series query.
     *  Notice that: query using timestamps, query time filter and value filter is not needed,
     *  but overflow time filter, insert data and overflow update true data is needed.
     *
     * @param commonTimestamps common timestamps calculated by filter
     * @return cross query result
     * @throws IOException file read error
     */
    public DynamicOneColumnData queryUsingTimestamps(long[] commonTimestamps) throws IOException {

        DynamicOneColumnData originalQueryData = queryOriginalDataUsingTimestamps(commonTimestamps);
        if (originalQueryData == null) {
            originalQueryData = new DynamicOneColumnData(dataType, true);
        }
        DynamicOneColumnData queryResult = new DynamicOneColumnData(dataType, true);

        int oldDataIdx = 0;
        for (long commonTime : commonTimestamps) {

            // the time in originalQueryData must in commonTimestamps
            if (oldDataIdx < originalQueryData.timeLength && originalQueryData.getTime(oldDataIdx) == commonTime) {
                boolean isOldDataAdoptedFlag = true;
                while (insertMemoryData.hasNext() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                    if (insertMemoryData.getCurrentMinTime() < commonTime) {
                        insertMemoryData.removeCurrentValue();
                    } else if (insertMemoryData.getCurrentMinTime() == commonTime) {
                        putMemoryDataToResult(queryResult, insertMemoryData);
                        insertMemoryData.removeCurrentValue();
                        oldDataIdx++;
                        isOldDataAdoptedFlag = false;
                        break;
                    }
                }

                if (!isOldDataAdoptedFlag) {
                    continue;
                }

                putFileDataToResult(queryResult, originalQueryData, oldDataIdx);

                oldDataIdx++;
            }

            // consider memory data
            while (insertMemoryData.hasNext() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                if (commonTime == insertMemoryData.getCurrentMinTime()) {
                    putMemoryDataToResult(queryResult, insertMemoryData);
                }
                insertMemoryData.removeCurrentValue();
            }
        }

        return queryResult;
    }

    private DynamicOneColumnData queryOriginalDataUsingTimestamps(long[] timestamps) throws IOException{
        if (timestamps.length == 0)
            return null;

        DynamicOneColumnData res = null;

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, queryTimeFilter);

        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                ValueReader valueReader = rowGroupReader.getValueReaders().get(measurementId);
                if (valueReader.getStartTime() > timestamps[timestamps.length - 1])
                    break;

                if (i == 0) {
                    res = rowGroupReader.readValueUseTimestamps(measurementId, timestamps);
                } else {
                    DynamicOneColumnData midResult = rowGroupReader.readValueUseTimestamps(measurementId, timestamps);
                    res.mergeRecord(midResult);
                }
            }
        }

        for (ValueReader valueReader : valueReaders) {
            if (valueReader.getStartTime() > timestamps[timestamps.length-1]) {
                break;
            }

            if (valueReader.getDataType().equals(dataType)) {
                DynamicOneColumnData midResult = valueReader.getValuesForGivenValues(timestamps);
                res.mergeRecord(midResult);
            }
        }

        return res;
    }

    private void putMemoryDataToResult(DynamicOneColumnData res, InsertDynamicData insertMemoryData) {
        res.putTime(insertMemoryData.getCurrentMinTime());

        switch (insertMemoryData.getDataType()) {
            case BOOLEAN:
                res.putBoolean(insertMemoryData.getCurrentBooleanValue());
                break;
            case INT32:
                res.putInt(insertMemoryData.getCurrentIntValue());
                break;
            case INT64:
                res.putLong(insertMemoryData.getCurrentLongValue());
                break;
            case FLOAT:
                res.putFloat(insertMemoryData.getCurrentFloatValue());
                break;
            case DOUBLE:
                res.putDouble(insertMemoryData.getCurrentDoubleValue());
                break;
            case TEXT:
                res.putBinary(insertMemoryData.getCurrentBinaryValue());
                break;
            default:
                throw new UnSupportedDataTypeException("UnuSupported DataType : " + insertMemoryData.getDataType());
        }
    }

    private void putFileDataToResult(DynamicOneColumnData queryResult, DynamicOneColumnData originalQueryData, int dataIdx) {

        long time = originalQueryData.getTime(dataIdx);

        while(overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().getRightBound() < time)
            overflowOperationReaderForFile.next();

        switch (dataType) {
            case BOOLEAN:
                if (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE) {
                        return;
                    }
                    queryResult.putTime(time);
                    queryResult.putBoolean(overflowOperationReaderForFile.getCurrentOperation().getValue().getBoolean());
                    return;
                }
                queryResult.putTime(time);
                queryResult.putBoolean(originalQueryData.getBoolean(dataIdx));
                break;
            case INT32:
                if (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE) {
                        return;
                    }
                    queryResult.putTime(time);
                    queryResult.putInt(overflowOperationReaderForFile.getCurrentOperation().getValue().getInt());
                    return;
                }
                queryResult.putTime(time);
                queryResult.putInt(originalQueryData.getInt(dataIdx));
                break;
            case INT64:
                if (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE) {
                        return;
                    }
                    queryResult.putTime(time);
                    queryResult.putLong(overflowOperationReaderForFile.getCurrentOperation().getValue().getLong());
                    return;
                }
                queryResult.putTime(time);
                queryResult.putLong(originalQueryData.getLong(dataIdx));
                break;
            case FLOAT:
                if (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE) {
                        return;
                    }
                    queryResult.putTime(time);
                    queryResult.putFloat(overflowOperationReaderForFile.getCurrentOperation().getValue().getFloat());
                    return;
                }
                queryResult.putTime(time);
                queryResult.putFloat(originalQueryData.getFloat(dataIdx));
                break;
            case DOUBLE:
                if (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE) {
                        return;
                    }
                    queryResult.putTime(time);
                    queryResult.putDouble(overflowOperationReaderForFile.getCurrentOperation().getValue().getDouble());
                    return;
                }
                queryResult.putTime(time);
                queryResult.putDouble(originalQueryData.getDouble(dataIdx));
                break;
            case TEXT:
                if (overflowOperationReaderForFile.hasNext() && overflowOperationReaderForFile.getCurrentOperation().verifyTime(time)) {
                    if (overflowOperationReaderForFile.getCurrentOperation().getType() == OverflowOperation.OperationType.DELETE) {
                        return;
                    }
                    queryResult.putTime(time);
                    queryResult.putBinary(overflowOperationReaderForFile.getCurrentOperation().getValue().getBinary());
                    return;
                }
                queryResult.putTime(time);
                queryResult.putBinary(originalQueryData.getBinary(dataIdx));
                break;
            default:
                throw new UnSupportedDataTypeException("UnuSupported DataType : " + insertMemoryData.getDataType());
        }
    }
}
