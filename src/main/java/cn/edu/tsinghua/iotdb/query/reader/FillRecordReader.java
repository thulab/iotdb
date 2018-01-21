package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.UnSupportedFillTypeException;
import cn.edu.tsinghua.iotdb.query.fill.FillProcessor;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class FillRecordReader extends RecordReader{
    public FillRecordReader(List<String> filePathList, String deltaObjectId, String measurementId, DynamicOneColumnData lastPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName, List<Object> overflowInfo) throws PathErrorException {
        super(filePathList, deltaObjectId, measurementId, lastPageInMemory, bufferWritePageList, compressionTypeName, overflowInfo);
    }

    public FillRecordReader(List<String> filePathList, String unsealedFilePath, List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectId, String measurementId, DynamicOneColumnData lastPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName, List<Object> overflowInfo) throws PathErrorException {
        super(filePathList, unsealedFilePath, rowGroupMetadataList, deltaObjectId, measurementId, lastPageInMemory, bufferWritePageList, compressionTypeName, overflowInfo);
    }

    /**
     * Get the time which is smaller than queryTime and is biggest and its value.
     *
     * @param beforeTime fill query start time
     * @param queryTime fill query end time
     * @param result fill query result
     * @throws IOException file read error
     */
    public void getPreviousFillResult(DynamicOneColumnData result, SingleSeriesFilterExpression fillTimeFilter, long beforeTime, long queryTime)
            throws IOException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(overflowTimeFilter, fillTimeFilter);

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                // get fill result in ValueReader
                if (FillProcessor.getPreviousFillResultInFile(result, rowGroupReader.getValueReaders().get(measurementId),
                        beforeTime, queryTime, mergeTimeFilter, overflowUpdate)) {
                    break;
                }
            }
        }

        // get fill result in InsertMemoryData
        FillProcessor.getPreviousFillResultInMemory(result, insertMemoryData, beforeTime, queryTime);

        if (result.valueLength == 0) {
            result.putEmptyTime(queryTime);
        } else {
            result.setTime(0, queryTime);
        }
    }

    /**
     * Get the time which is smaller than queryTime and is biggest and its value.
     *
     * @param beforeTime fill query start time
     * @param queryTime fill query end time
     * @param result fill query result
     * @throws IOException file read error
     */
    public void getLinearFillResult(DynamicOneColumnData result, SingleSeriesFilterExpression fillTimeFilter,
                                    long beforeTime, long queryTime, long afterTime) throws IOException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(overflowTimeFilter, fillTimeFilter);

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // has get fill result in ValueReader
                if (FillProcessor.getLinearFillResultInFile(result, rowGroupReader.getValueReaders().get(measurementId), beforeTime, queryTime, afterTime,
                        mergeTimeFilter, overflowUpdate)) {
                    break;
                }
            }
        }

        // get fill result in InsertMemoryData
        FillProcessor.getLinearFillResultInMemory(result, insertMemoryData, beforeTime, queryTime, afterTime);

        if (result.timeLength == 0) {
            result.putEmptyTime(queryTime);
        } else if (result.valueLength == 1) {
            // only has previous or after time
            if (result.getTime(0) != queryTime) {
                result.timeLength = result.valueLength = 0;
                result.putEmptyTime(queryTime);
            }
        } else {
            // startTime and endTime will not be equals to queryTime
            long startTime = result.getTime(0);
            long endTime = result.getTime(1);

            switch (result.dataType) {
                case INT32:
                    int startIntValue = result.getInt(0);
                    int endIntValue = result.getInt(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    int fillIntValue = startIntValue + (int)((double)(endIntValue-startIntValue)/(double)(endTime-startTime)*(double)(queryTime-startTime));
                    result.setInt(0, fillIntValue);
                    break;
                case INT64:
                    long startLongValue = result.getLong(0);
                    long endLongValue = result.getLong(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    long fillLongValue = startLongValue + (long)((double)(endLongValue-startLongValue)/(double)(endTime-startTime)*(double)(queryTime-startTime));
                    result.setLong(0, fillLongValue);
                    break;
                case FLOAT:
                    float startFloatValue = result.getFloat(0);
                    float endFloatValue = result.getFloat(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    float fillFloatValue = startFloatValue + (float)((endFloatValue-startFloatValue)/(endTime-startTime)*(queryTime-startTime));
                    result.setFloat(0, fillFloatValue);
                    break;
                case DOUBLE:
                    double startDoubleValue = result.getDouble(0);
                    double endDoubleValue = result.getDouble(1);
                    result.timeLength = result.valueLength = 1;
                    result.setTime(0, queryTime);
                    double fillDoubleValue = startDoubleValue + (double)((endDoubleValue-startDoubleValue)/(endTime-startTime)*(queryTime-startTime));
                    result.setDouble(0, fillDoubleValue);
                    break;
                default:
                    throw new UnSupportedFillTypeException("Unsupported linear fill data type : " + result.dataType);

            }
        }
    }
}
