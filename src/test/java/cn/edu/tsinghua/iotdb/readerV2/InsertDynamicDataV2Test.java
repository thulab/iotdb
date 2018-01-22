package cn.edu.tsinghua.iotdb.readerV2;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.query.v2.InsertDynamicData;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.*;

public class InsertDynamicDataV2Test {

    private String deltaObjectId = "device";
    private String measurementId = "sensor";
    private MeasurementDescriptor descriptor = new MeasurementDescriptor(measurementId, TSDataType.FLOAT, TSEncoding.RLE);
    private FilterSeries<Long> timeSeries = timeFilterSeries();
    private FilterSeries<Float> valueSeries = floatFilterSeries(deltaObjectId, measurementId, FilterSeriesType.VALUE_FILTER);
    SingleSeriesFilterExpression timeFilter = ltEq(timeSeries, 560L, true);
    SingleSeriesFilterExpression valueFilter = gtEq(valueSeries, -300.0f, true);

    @Test
    public void queryTest() {
//        InsertDynamicData reader = new InsertDynamicData(TSDataType.INT32, timeFilter, valueFilter,
//                new FakedRawSeriesChunk(), new OverflowInsertDataReader(1L, new FakedOverflowInsertDataReader(new long[]{1,2,3})),
//                new FakedOverflowUpdateOperationReader());
    }

    public static class FakedOverflowInsertDataReader implements SeriesReader {

        private long[] timestamps;
        private int index;
        private long value;

        public FakedOverflowInsertDataReader(long[] timestamps) {
            this.timestamps = timestamps;
            index = 0;
            value = 1L;
        }

        public FakedOverflowInsertDataReader(long[] timestamps, long value) {
            this.timestamps = timestamps;
            index = 0;
            this.value = value;
        }

        @Override
        public boolean hasNext() throws IOException {
            return index < timestamps.length;
        }

        @Override
        public TimeValuePair next() throws IOException {
            return new TimeValuePair(timestamps[index++], new TsPrimitiveType.TsLong(value));
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            next();
        }

        @Override
        public void close() throws IOException {

        }
    }

    public static class FakedRawSeriesChunk implements RawSeriesChunk {

        @Override
        public TSDataType getDataType() {
            return null;
        }

        @Override
        public long getMaxTimestamp() {
            return 0;
        }

        @Override
        public long getMinTimestamp() {
            return 0;
        }

        @Override
        public TsPrimitiveType getMaxValue() {
            return null;
        }

        @Override
        public TsPrimitiveType getMinValue() {
            return null;
        }

        @Override
        public Iterator<TimeValuePair> getIterator() {
            return null;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

    public static class FakedOverflowUpdateOperationReader implements OverflowOperationReader {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public OverflowOperation next() {
            return null;
        }

        @Override
        public OverflowOperation getCurrentOperation() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
