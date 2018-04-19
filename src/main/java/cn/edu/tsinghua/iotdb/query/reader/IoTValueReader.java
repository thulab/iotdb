package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.LongFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IoTValueReader extends ValueReader {

    private long maxTombstoneTime;
    private String deltaObjectId;
    private String measurementId;

    public IoTValueReader(long offset, long totalSize, TSDataType dataType, TsDigest digest,
                          ITsRandomAccessFileReader raf, List<String> enumValues, CompressionTypeName compressionTypeName,
                          long rowNums, long startTime, long endTime, long maxTombstoneTime, String deltaObjectId, String measurementId) {
        super(offset, totalSize, dataType, digest, raf, enumValues, compressionTypeName, rowNums, startTime, endTime);
        this.maxTombstoneTime = maxTombstoneTime;
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    @Override
    public DynamicOneColumnData readOneColumnUseFilter(DynamicOneColumnData res, int fetchSize, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        // convert tombstone to a filter
        if (maxTombstoneTime >= this.getStartTime()) {
            if(timeFilter == null) {
                timeFilter = new GtEq<Long>(new LongFilterSeries(deltaObjectId, measurementId, TSDataType.INT64, FilterSeriesType.TIME_FILTER),
                        maxTombstoneTime, false);
            } else {
                timeFilter = new And(timeFilter, new GtEq<Long>(new LongFilterSeries(deltaObjectId, measurementId, TSDataType.INT64, FilterSeriesType.TIME_FILTER),
                        maxTombstoneTime, false));
            }
        }
        return super.readOneColumnUseFilter(res, fetchSize, timeFilter, freqFilter, valueFilter);
    }

    @Override
    public DynamicOneColumnData getValuesForGivenValues(long[] timestamps) throws IOException {
        // remove time stamps less than maxTombstoneTime
        if(maxTombstoneTime > 0) {
            List<Long> timeList = new ArrayList<>();
            for(long time : timestamps) {
                if(time > maxTombstoneTime) {
                    timeList.add(time);
                }
            }
            timestamps = new long[timeList.size()];
            for(int i = 0; i < timeList.size(); i++)
                timestamps[i] = timeList.get(i);
        }
        return super.getValuesForGivenValues(timestamps);
    }

    @Override
    public boolean columnSatisfied(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression timeFilter) {
        return getEndTime() > maxTombstoneTime && super.columnSatisfied(valueFilter, freqFilter, timeFilter);
    }

    @Override
    public boolean pageSatisfied(DigestForFilter timeDigestFF, DigestForFilter valueDigestFF, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter) {
        return (Long) timeDigestFF.getMaxValue() > maxTombstoneTime && super.pageSatisfied(timeDigestFF, valueDigestFF, timeFilter, valueFilter, freqFilter);
    }
}
