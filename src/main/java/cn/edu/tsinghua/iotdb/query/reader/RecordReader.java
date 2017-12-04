package cn.edu.tsinghua.iotdb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * This class implements several read methods which can read data in different ways.<br>
 * A RecordReader only represents a (deltaObject, measurement).
 * This class provides some APIs for reading.
 *
 */
public class RecordReader {

    static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    private String deltaObjectUID, measurementID;

    /** compression type in this series **/
    public CompressionTypeName compressionTypeName;

    /** ReaderManager for current (deltaObjectUID, measurementID) **/
    private ReaderManager readerManager;

    /** for lock **/
    private int lockToken;

    /** bufferwrite data, the data page in memory **/
    public DynamicOneColumnData insertPageInMemory;

    /** bufferwrite data, the unsealed page **/
    public List<ByteArrayInputStream> bufferWritePageList;

    /** insertPageInMemory + bufferWritePageList; **/
    public InsertDynamicData insertAllData;

    /** overflow data **/
    public List<Object> overflowInfo;

    /**
     * @param filePathList bufferwrite file has been serialized completely
     * @throws IOException file error
     */
    public RecordReader(List<String> filePathList, String deltaObjectUID, String measurementID, int lockToken,
                        DynamicOneColumnData insertPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws IOException {
        this.readerManager = new ReaderManager(filePathList);
        this.deltaObjectUID = deltaObjectUID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.insertPageInMemory = insertPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.overflowInfo = overflowInfo;
    }

    /**
     * @param filePathList       bufferwrite file has been serialized completely
     * @param unsealedFilePath   unsealed file reader
     * @param rowGroupMetadataList unsealed RowGroupMetadataList to construct unsealedFileReader
     * @throws IOException file error
     */
    public RecordReader(List<String> filePathList, String unsealedFilePath,
                        List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectUID, String measurementID, int lockToken,
                        DynamicOneColumnData insertPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws IOException {
        this.readerManager = new ReaderManager(filePathList, unsealedFilePath, rowGroupMetadataList);
        this.deltaObjectUID = deltaObjectUID;
        this.measurementID = measurementID;
        this.lockToken = lockToken;
        this.insertPageInMemory = insertPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.overflowInfo = overflowInfo;
    }

    /**
     * Read one series with overflow and bufferwrite, no filter.
     *
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData queryOneSeries(String deltaObjectId, String measurementId,
                                               DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                               SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter, DynamicOneColumnData res, int fetchSize)
            throws ProcessorException, IOException, PathErrorException {

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> dbRowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, timeFilter);
        int rowGroupIndex = 0;
        if (res != null) {
            rowGroupIndex = res.getRowGroupIndex();
        }

        // iterative res, res may be expand
        for (; rowGroupIndex < dbRowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader dbRowGroupReader = dbRowGroupReaderList.get(rowGroupIndex);
            if (dbRowGroupReader.getValueReaders().containsKey(measurementId) &&
                    dbRowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                res = ValueReaderProcessor.getValuesWithOverFlow(dbRowGroupReader.getValueReaders().get(measurementId),
                        updateTrue, updateFalse, insertMemoryData, timeFilter, null, valueFilter, res, fetchSize);
                if (res.valueLength >= fetchSize) {
                    return res;
                }
            }
        }

        if (res == null) {
            res = new DynamicOneColumnData(dataType, true);
        }

        // add left insert values
        if (insertMemoryData.hasInsertData()) {
            res.hasReadAll = addLeftInsertValue(res, insertMemoryData, fetchSize, timeFilter, updateTrue, updateFalse);
        } else {
            res.hasReadAll = true;
        }
        return res;
    }

    /**
     * Aggregation calculate function of <code>RecordReader</code> without filter.
     *
     * @param deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param aggregateFunction aggregation function
     * @param updateTrue update operation which satisfies the filter
     * @param updateFalse update operation which doesn't satisfy the filter
     * @param insertMemoryData memory bufferwrite insert data
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     * @return aggregation result
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public AggregateFunction aggregate(String deltaObjectId, String measurementId, AggregateFunction aggregateFunction,
                                       DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                                       SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter
    ) throws ProcessorException, IOException, PathErrorException {

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, timeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                ValueReaderProcessor.aggregate(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, insertMemoryData, updateTrue, updateFalse, timeFilter, freqFilter, valueFilter);
            }
        }

        // add left insert values
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            aggregateFunction.calculateValueFromLeftMemoryData(insertMemoryData);
        }

        return aggregateFunction;
    }

    /**
     * <p>
     * Calculate the aggregate result using the given timestamps.
     * Return a pair of AggregationResult and Boolean, AggregationResult represents the aggregation result,
     * Boolean represents that whether there still has unread data.
     *
     * @param deltaObjectId deltaObjectId deltaObjectId of <code>Path</code>
     * @param measurementId measurementId of <code>Path</code>
     * @param aggregateFunction aggregation function
     * @param updateTrue update operation which satisfies the filter
     * @param updateFalse update operation which doesn't satisfy the filter
     * @param insertMemoryData memory bufferwrite insert data
     * @param overflowTimeFilter time filter
     * @param freqFilter frequency filter
     * @param timestamps timestamps calculated by the cross filter
     * @return aggregation result and whether still has unread data
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public Pair<AggregateFunction, Boolean> aggregateUsingTimestamps(
            String deltaObjectId, String measurementId, AggregateFunction aggregateFunction,
            DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
            SingleSeriesFilterExpression overflowTimeFilter, SingleSeriesFilterExpression freqFilter, List<Long> timestamps)
            throws ProcessorException, IOException, PathErrorException {

        boolean stillHasUnReadData;

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);

        int commonTimestampsIndex = 0;

        int rowGroupIndex = aggregateFunction.resultData.rowGroupIndex;

        for (; rowGroupIndex < rowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // TODO commonTimestampsIndex could be saved as a parameter

                commonTimestampsIndex = ValueReaderProcessor.aggregateUsingTimestamps(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, insertMemoryData, updateTrue, updateFalse, overflowTimeFilter, freqFilter, timestamps);

                // all value of commonTimestampsIndex has been used,
                // the next batch of commonTimestamps should be loaded
                if (commonTimestampsIndex >= timestamps.size()) {
                    return new Pair<>(aggregateFunction, true);
                }
            }
        }

        // calculate aggregation using unsealed file data and memory data
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            stillHasUnReadData = aggregateFunction.calcAggregationUsingTimestamps(insertMemoryData, timestamps, commonTimestampsIndex);
        } else {
            if (commonTimestampsIndex < timestamps.size()) {
                stillHasUnReadData = false;
            } else {
                stillHasUnReadData = true;
            }
        }

        return new Pair<>(aggregateFunction, stillHasUnReadData);
    }

    /**
     *  This function is used for cross column query.
     *
     * @param deltaObjectId
     * @param measurementId
     * @param overflowTimeFilter overflow time filter for this query path
     * @param commonTimestamps
     * @param insertMemoryData
     * @return
     * @throws ProcessorException
     * @throws IOException
     */
    public DynamicOneColumnData queryUsingTimestamps(String deltaObjectId, String measurementId,
                                                     SingleSeriesFilterExpression overflowTimeFilter, long[] commonTimestamps,
                                                     InsertDynamicData insertMemoryData)
            throws ProcessorException, IOException, PathErrorException {

        TSDataType dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
        SingleValueVisitor filterVerifier = null;
        if (overflowTimeFilter != null) {
            filterVerifier = new SingleValueVisitor(overflowTimeFilter);
        }

        DynamicOneColumnData originalQueryData = queryOriginalDataUsingTimestamps(deltaObjectId, measurementId, overflowTimeFilter, commonTimestamps);
        if (originalQueryData == null) {
            originalQueryData = new DynamicOneColumnData(dataType, true);
        }
        DynamicOneColumnData newQueryData = new DynamicOneColumnData(dataType, true);

        int oldDataIdx = 0;
        for (long commonTime : commonTimestamps) {

            // the time in originalQueryData must in commonTimestamps
            if (oldDataIdx < originalQueryData.timeLength && originalQueryData.getTime(oldDataIdx) == commonTime) {

                if (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                    if (insertMemoryData.getCurrentMinTime() == commonTime) {
                        newQueryData.putTime(insertMemoryData.getCurrentMinTime());
                        putValueFromMemoryData(newQueryData, insertMemoryData);
                        insertMemoryData.removeCurrentValue();
                        oldDataIdx++;
                        continue;
                    } else {
                        insertMemoryData.removeCurrentValue();
                    }
                }

                if (overflowTimeFilter == null || filterVerifier.verify(commonTime)) {
                    newQueryData.putTime(commonTime);
                    newQueryData.putAValueFromDynamicOneColumnData(originalQueryData, oldDataIdx);
                }

                oldDataIdx++;
            }

            // consider memory data
            while (insertMemoryData != null && insertMemoryData.hasInsertData() && insertMemoryData.getCurrentMinTime() <= commonTime) {
                if (commonTime == insertMemoryData.getCurrentMinTime()) {
                    newQueryData.putTime(insertMemoryData.getCurrentMinTime());
                    putValueFromMemoryData(newQueryData, insertMemoryData);
                }
                insertMemoryData.removeCurrentValue();
            }
        }

        return newQueryData;
    }

    private DynamicOneColumnData queryOriginalDataUsingTimestamps(String deltaObjectId, String measurementId,
                                                                  SingleSeriesFilterExpression overflowTimeFilter, long[] timestamps)
            throws IOException {

        DynamicOneColumnData res = null;

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);
        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            RowGroupReader dbRowGroupReader = rowGroupReaderList.get(i);
            if (i == 0) {
                res = dbRowGroupReader.readValueUseTimestamps(measurementId, timestamps);
            } else {
                DynamicOneColumnData tmpRes = dbRowGroupReader.readValueUseTimestamps(measurementId, timestamps);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }

    /**
     * Calculate the left value in memory.
     *
     * @param res result answer
     * @param insertMemoryData memory data
     * @param fetchSize read fetch size
     * @param timeFilter filter to select time
     * @param updateTrue <code>DynamicOneColumnData</code> represents which value to update to new value
     * @param updateFalse <code>DynamicOneColumnData</code> represents which value of update to new value is
     *                    not satisfied with the filter
     * @return true represents that all the values has been read
     * @throws IOException TsFile read error
     */
    private boolean addLeftInsertValue(DynamicOneColumnData res, InsertDynamicData insertMemoryData, int fetchSize,
                                       SingleSeriesFilterExpression timeFilter, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse) throws IOException {
        SingleValueVisitor<?> timeVisitor = null;
        if (timeFilter != null) {
            timeVisitor = new SingleValueVisitor(timeFilter);
        }
        long maxTime;
        if (res.valueLength > 0) {
            maxTime = res.getTime(res.valueLength - 1);
        } else {
            maxTime = -1;
        }

        while (insertMemoryData.hasInsertData()) {
            long curTime = insertMemoryData.getCurrentMinTime(); // current insert time
            if (maxTime < curTime) {
                res.putTime(curTime);
                putValueFromMemoryData(res, insertMemoryData);
                insertMemoryData.removeCurrentValue();
            }
            // when the length reach to fetchSize, stop put values and return false
            if (res.valueLength >= fetchSize) {
                return false;
            }
        }
        return true;
    }

    private void putValueFromMemoryData(DynamicOneColumnData res, InsertDynamicData insertMemoryData) {
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

    /**
     * Close current RecordReader.
     *
     * @throws IOException
     * @throws ProcessorException
     */
    public void close() throws IOException, ProcessorException {
        readerManager.close();
        // unlock for one subQuery
        ReadLockManager.getInstance().unlockForSubQuery(deltaObjectUID, measurementID, lockToken);
    }
}
