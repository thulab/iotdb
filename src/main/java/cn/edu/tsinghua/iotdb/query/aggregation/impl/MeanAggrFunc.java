package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.util.List;

public class MeanAggrFunc extends AggregateFunction{

    double sum = 0.0;
    int cnt = 0;


    public MeanAggrFunc() {
        super(AggregationConstant.MEAN, TSDataType.DOUBLE);
    }

    @Override
    public void putDefaultValue() {
        resultData.putEmptyTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
    // TODO ：make use of this?
        if(resultData.timeLength == 0)
            resultData.putTime(0);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        // TODO : update mean or update sum?
        if(resultData.timeLength == 0)
            resultData.putTime(0);
        updateMean(dataInThisPage);
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if(resultData.timeLength == 0)
            resultData.putTime(0);
        updateMean(insertMemoryData);
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }
        // use switch here to reduce switch usage to once
        switch (insertMemoryData.getDataType()) {
            case INT32:
               updateMeanWithInt(insertMemoryData, timestamps, timeIndex);
               break;
            case INT64:
                updateMeanWithLong(insertMemoryData, timestamps, timeIndex);
                break;
            case FLOAT:
                updateMeanWithFloat(insertMemoryData, timestamps, timeIndex);
                break;
            case DOUBLE:
                updateMeanWithDouble(insertMemoryData, timestamps, timeIndex);
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
                throw new ProcessorException("Unsupported data type in aggregation MEAN : " + dataType);
        }

        return insertMemoryData.hasInsertData();
    }

    @Override
    public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        if (resultData.emptyTimeLength == 0) {
            if (resultData.timeLength == 0) {
                resultData.putEmptyTime(partitionStart);
            } else if (resultData.getTime(resultData.timeLength - 1) != partitionStart) {
                resultData.putEmptyTime(partitionStart);
            }
        } else {
            if ((resultData.getEmptyTime(resultData.emptyTimeLength - 1) != partitionStart)
                    && (resultData.timeLength == 0 ||
                    (resultData.timeLength > 0 && resultData.getTime(resultData.timeLength - 1) != partitionStart)))
                resultData.putEmptyTime(partitionStart);
        }


        // use switch here to reduce switch usage to once
        switch (data.dataType) {
            case INT32:
                groupUpdateMeanWithInt(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case INT64:
                groupUpdateMeanWithLong(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case FLOAT:
                groupUpdateMeanWithFloat(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case DOUBLE:
                groupUpdateMeanWithDouble(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
                try {
                    throw new ProcessorException("Unsupported data type in aggregation MEAN : " + dataType);
                } catch (ProcessorException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     *  Update sum use all data in a column.
     * @param data
     * @throws ProcessorException
     */
    private void updateMean(DynamicOneColumnData data) throws ProcessorException {
        switch (data.dataType) {
            case INT32:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getInt(data.curIdx);
                    cnt++;
                }
            case INT64:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getLong(data.curIdx);
                    cnt++;
                }
                break;
            case FLOAT:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getFloat(data.curIdx);
                    cnt++;
                }
                break;
            case DOUBLE:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getDouble(data.curIdx);
                    cnt++;
                }
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
                throw new ProcessorException("Unsupported data type in aggregation MEAN : " + dataType);
        }
        resultData.setDouble(0, sum / cnt);
    }

    // TODO : use function template generator
    /**
     * Update mean using values in a column whose timestamp is contained in timestamps[timeIndex:].
     * Use different method to avoid redundant switches.
     * @param insertMemoryData
     * @param timestamps
     * @param timeIndex
     * @throws IOException
     */
    private void updateMeanWithInt(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    int val = insertMemoryData.getCurrentIntValue();
                    sum += val;
                    cnt ++;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        resultData.setDouble(0, sum / cnt);
    }

    private void updateMeanWithLong(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    Long val = insertMemoryData.getCurrentLongValue();
                    sum += val;
                    cnt ++;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        resultData.setDouble(0, sum / cnt);
    }

    private void updateMeanWithFloat(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    float val = insertMemoryData.getCurrentFloatValue();
                    sum += val;
                    cnt ++;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        resultData.setDouble(0, sum / cnt);
    }

    private void updateMeanWithDouble(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    double val = insertMemoryData.getCurrentDoubleValue();
                    sum += val;
                    cnt ++;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        resultData.setDouble(0, sum / cnt);
    }

    /**
     *  Update mean using data that fall in given window, the window is the intersection of [partitionStart, partitionEnd]
     *  and [intervalStart, intervalEnd].
     * @param partitionStart
     * @param partitionEnd
     * @param intervalStart
     * @param intervalEnd
     * @param data
     */
    private void groupUpdateMeanWithInt(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                int val = data.getInt(data.curIdx);
                sum += val;
                cnt ++;
                data.curIdx++;
            }
        }
    }

    private void groupUpdateMeanWithLong(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                long val = data.getLong(data.curIdx);
                sum += val;
                cnt ++;
                data.curIdx++;
            }
        }
    }

    private void groupUpdateMeanWithFloat(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                float val = data.getFloat(data.curIdx);
                sum += val;
                cnt ++;
                data.curIdx++;
            }
        }
    }

    private void groupUpdateMeanWithDouble(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                double val = data.getDouble(data.curIdx);
                sum += val;
                cnt ++;
                data.curIdx++;
            }
        }
    }
}
