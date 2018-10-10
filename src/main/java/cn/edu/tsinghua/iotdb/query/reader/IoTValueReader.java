package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.LongFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is an extension of ValueReader that will use the tombstone as an additional filter.
 */
public class IoTValueReader extends ValueReader {

    /**
     * The max deletion time of the tombstones that has effect on this timeseries.
     */
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

    /**
     * Construct an additional filter using maxTombstoneTime.
     * @param res
     * @param fetchSize
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return
     * @throws IOException
     */
    @Override
    public DynamicOneColumnData readOneColumnUseFilter(DynamicOneColumnData res, int fetchSize,
                                                       SingleSeriesFilterExpression timeFilter,
                                                       SingleSeriesFilterExpression freqFilter,
                                                       SingleSeriesFilterExpression valueFilter) throws IOException {
        // convert tombstone to a filter
        if (maxTombstoneTime >= this.getStartTime()) {
            if(timeFilter == null) {
                timeFilter = new GtEq<Long>(FilterFactory.timeFilterSeries(),
                        maxTombstoneTime, false);
            } else {
                timeFilter = new And(timeFilter, new GtEq<Long>(FilterFactory.timeFilterSeries(),
                        maxTombstoneTime, false));
            }
        }
        return super.readOneColumnUseFilter(res, fetchSize, timeFilter, freqFilter, valueFilter);
    }

    /**
     * Additionally remove timestamps that is less than or equal to maxTombstoneTime.
     * @param timestamps
     * @return
     * @throws IOException
     */
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

    /**
     * First judge with maxTombstoneTime, then call super method.
     * @param valueFilter
     * @param freqFilter
     * @param timeFilter
     * @return
     */
    @Override
    public boolean columnSatisfied(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                   SingleSeriesFilterExpression timeFilter) {
        return getEndTime() > maxTombstoneTime && super.columnSatisfied(valueFilter, freqFilter, timeFilter);
    }

    /**
     * First judge with maxTombstoneTime, then call super method.
     * @param timeDigestFF
     * @param valueDigestFF
     * @param timeFilter
     * @param valueFilter
     * @param freqFilter
     * @return
     */
    @Override
    public boolean pageSatisfied(DigestForFilter timeDigestFF, DigestForFilter valueDigestFF,
                                 SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                 SingleSeriesFilterExpression freqFilter) {
        return (Long) timeDigestFF.getMaxValue() > maxTombstoneTime && super.pageSatisfied(timeDigestFF, valueDigestFF, timeFilter, valueFilter, freqFilter);
    }
}
