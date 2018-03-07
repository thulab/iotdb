package cn.edu.tsinghua.iotdb.query.aggregationv2.impl;

import cn.edu.tsinghua.iotdb.query.aggregationv2.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregationv2.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.v2.InsertDynamicData;
import cn.edu.tsinghua.iotdb.udf.AbstractUDSF;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.util.List;

public class MinTimeAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MinTimeAggrFunc() {
        super(AggregationConstant.MIN_TIME, TSDataType.INT64);
    }

    @Override
    public void putDefaultValue() {
        resultData.putEmptyTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        long timestamp = pageHeader.data_page_header.min_timestamp;
        updateMinTime(timestamp);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        if (dataInThisPage.valueLength == 0) {
            return;
        }
        // use the first timestamp of the DynamicOneColumnData
        long timestamp = dataInThisPage.getTime(0);
        updateMinTime(timestamp);
    }
    

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        while (insertMemoryData.hasNext()) {
            long time = insertMemoryData.getCurrentMinTime();
            updateMinTime(time);
            insertMemoryData.removeCurrentValue();
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex)
            throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasNext()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    updateMinTime(timestamps.get(timeIndex));
                    return false;
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }

        return insertMemoryData.hasNext();
    }

    @Override
    public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd,
                                       DynamicOneColumnData data) {
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

        long minTime = Long.MAX_VALUE;
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                if (minTime > data.getTime(data.curIdx)) {
                    minTime = data.getTime(data.curIdx);
                }
                data.curIdx++;
            }
        }

        if (minTime != Long.MAX_VALUE) {
            if (resultData.emptyTimeLength > 0 && resultData.getEmptyTime(resultData.emptyTimeLength - 1) == partitionStart) {
                resultData.removeLastEmptyTime();
                resultData.putTime(partitionStart);
                resultData.putLong(minTime);
            } else {
                if (minTime < resultData.getLong(resultData.valueLength - 1)) {
                    resultData.setLong(resultData.valueLength - 1, minTime);
                }
            }
        }
    }

    @Override
    public void calcSegmentByAggregation(int segmentIdx, AbstractUDSF udsf, DynamicOneColumnData data) throws ProcessorException {
        if (resultData.timeLength == segmentIdx) {
            resultData.putTime(data.getTime(data.curIdx));
        }

        long minTime = Long.MAX_VALUE;
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            Comparable<?> value = data.getAnObject(data.curIdx);

            if (udsf.isBreakpoint(time, value)) {
                resultData.putEmptyTime(udsf.getLastTime());
                if (resultData.valueLength - 1 == segmentIdx) {
                    long preTime = resultData.getLong(segmentIdx);
                    minTime = minTime < preTime ? minTime : preTime;
                    resultData.setLong(segmentIdx, minTime);
                } else {
                    resultData.putLong(minTime);
                }

                udsf.setLastTime(time);
                udsf.setLastValue(value);
                return;
            }

            minTime = minTime < time ? minTime : time;
            data.curIdx++;
            udsf.setLastTime(time);
            udsf.setLastValue(value);
        }

        if (resultData.valueLength - 1 == segmentIdx) {
            long preTime = resultData.getLong(segmentIdx);
            minTime = minTime < preTime ? minTime : preTime;
            resultData.setLong(segmentIdx, minTime);
        } else {
            resultData.putLong(minTime);
        }
    }

    private void updateMinTime(long timestamp) {
        if (!hasSetValue) {
            resultData.putLong(timestamp);
            hasSetValue = true;
        } else {
            long mint = resultData.getLong(0);
            mint = mint < timestamp ? mint : timestamp;
            resultData.setLong(0, mint);
        }
    }
}
