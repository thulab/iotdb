package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;


public class MinValueAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MinValueAggrFunc(TSDataType dataType) {
        super(AggregationConstant.MIN_VALUE, dataType, true);
    }

    @Override
    public void putDefaultValue() {
        result.data.putEmptyTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        Digest pageDigest = pageHeader.data_page_header.getDigest();
        DigestForFilter digest = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);
        Comparable<?> minv = digest.getMinValue();
        updateMinValue(minv);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        if (dataInThisPage.valueLength == 0) {
            return;
        }
        Comparable<?> minv = getMinValue(dataInThisPage);
        updateMinValue(minv);
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        Object min_value = insertMemoryData.calcAggregation(AggregationConstant.MIN_VALUE);
        if (min_value != null) {
            updateMinValue((Comparable<?>) min_value);
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex)
            throws IOException, ProcessorException {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    Object value = insertMemoryData.getCurrentObjectValue();
                    updateMinValue((Comparable<?>) value);
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

        return insertMemoryData.hasInsertData();
    }

    @Override
    public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        if (result.data.emptyTimeLength == 0) {
            if (result.data.timeLength == 0) {
                result.data.putEmptyTime(partitionStart);
            } else if (result.data.getTime(result.data.timeLength - 1) != partitionStart) {
                result.data.putEmptyTime(partitionStart);
            }
        } else {
            if ((result.data.getEmptyTime(result.data.emptyTimeLength - 1) != partitionStart)
                    && (result.data.timeLength == 0 ||
                    (result.data.timeLength > 0 && result.data.getTime(result.data.timeLength - 1) != partitionStart)))
                result.data.putEmptyTime(partitionStart);
        }

        Comparable<?> minValue = null;
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                if (minValue == null) {
                    minValue = data.getAnObject(data.curIdx);
                } else {
                    if (compare(minValue, data.getAnObject(data.curIdx)) > 0) {
                        minValue = data.getAnObject(data.curIdx);
                    }
                }
                data.curIdx++;
            }
        }

        if (minValue != null) {
            if (result.data.emptyTimeLength > 0 && result.data.getEmptyTime(result.data.emptyTimeLength - 1) == partitionStart) {
                result.data.removeLastEmptyTime();
                result.data.putTime(partitionStart);
                result.data.putAnObject(minValue);
            } else {
                Comparable<?> v = result.data.getAnObject(result.data.valueLength - 1);
                if (compare(minValue, v) < 0) {
                    result.data.setAnObject(result.data.valueLength - 1, minValue);
                }
            }
        }
    }

    private void updateMinValue(Comparable<?> minv) {
        if (!hasSetValue) {
            result.data.putAnObject(minv);
            hasSetValue = true;
        } else {
            Comparable<?> v = result.data.getAnObject(0);
            if (compare(v, minv) > 0) {
                result.data.setAnObject(0, minv);
            }
        }
    }

    private Comparable<?> getMinValue(DynamicOneColumnData dataInThisPage) {
        Comparable<?> v = dataInThisPage.getAnObject(0);
        for (int i = 1; i < dataInThisPage.valueLength; i++) {
            Comparable<?> nextV = dataInThisPage.getAnObject(i);
            if (compare(v, nextV) > 0) {
                v = nextV;
            }
        }
        return v;
    }

    private int compare(Comparable<?> o1, Comparable<?> o2) {
        switch (dataType) {
            case BOOLEAN:
                return ((Boolean) o1).compareTo((Boolean) o2);
            case INT32:
                return ((Integer) o1).compareTo((Integer) o2);
            case INT64:
                return ((Long) o1).compareTo((Long) o2);
            case FLOAT:
                return ((Float) o1).compareTo((Float) o2);
            case DOUBLE:
                return ((Double) o1).compareTo((Double) o2);
            case TEXT:
                return ((Binary) o1).compareTo((Binary) o2);
            default:
                throw new UnSupportedDataTypeException("Aggregation UnSupportDataType: " + dataType);
        }
    }

}
