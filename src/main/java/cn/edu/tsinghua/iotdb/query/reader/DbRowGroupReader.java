//package cn.edu.tsinghua.iotdb.query.reader;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
//import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
//import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
//import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
//import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
//import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
//import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
//
//public class DbRowGroupReader extends RowGroupReader {
//
//    protected static final Logger logger = LoggerFactory.getLogger(DbRowGroupReader.class);
//
//    public Map<String, TSDataType> seriesTypeMap;
//    private Map<String, ValueReader> valueReaders = new HashMap<>();
//    private String deltaObjectUID, deltaObjectType;
//
//    protected ArrayList<String> sids;
//    private long totalByteSize;
//
//    protected ITsRandomAccessFileReader raf;
//
//    public DbRowGroupReader(RowGroupMetaData rowGroupMetaData, ITsRandomAccessFileReader raf) {
//        logger.debug("init a new DbRowGroupReader..");
//        seriesTypeMap = new HashMap<>();
//        deltaObjectUID = rowGroupMetaData.getDeltaObjectID();
//        sids = new ArrayList<>();
//        deltaObjectType = rowGroupMetaData.getDeltaObjectType();
//        this.totalByteSize = rowGroupMetaData.getTotalByteSize();
//        this.raf = raf;
//
//        for (TimeSeriesChunkMetaData tscMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
//            if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
//                sids.add(tscMetaData.getProperties().getMeasurementUID());
//                seriesTypeMap.put(tscMetaData.getProperties().getMeasurementUID(),
//                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType());
//
//                OverflowBufferWriteProcessor si = new OverflowBufferWriteProcessor(tscMetaData.getProperties().getFileOffset(),
//                        tscMetaData.getTotalByteSize(),
//                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
//                        tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), this.raf,
//                        tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
//                        tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows());
//                valueReaders.put(tscMetaData.getProperties().getMeasurementUID(), si);
//            }
//        }
//    }
//
//    public List<Object> getTimeByRet(List<Object> timeRet, HashMap<Integer, Object> retMap) {
//        List<Object> timeRes = new ArrayList<>();
//        for (Integer i : retMap.keySet()) {
//            timeRes.add(timeRet.get(i));
//        }
//        return timeRes;
//    }
//
//    public String getDeltaObjectType() {
//        return this.deltaObjectType;
//    }
//
//    public TSDataType getDataTypeBySeriesName(String name) {
//        return this.seriesTypeMap.get(name);
//    }
//
//    public String getDeltaObjectUID() {
//        return this.deltaObjectUID;
//    }
//
//    /**
//     * Read time-value pairs whose time is included in timeRet.
//     * WARNING: this function is only for "time" Series
//     *
//     * @param measurementId id of measurement
//     * @param timeRet       array of the time.
//     * @throws IOException
//     */
//    public DynamicOneColumnData readValueUseTimestamps(String measurementId, long[] timeRet) throws IOException {
//        return valueReaders.get(measurementId).getValuesForGivenValues(timeRet);
//    }
//
//    public DynamicOneColumnData readOneColumnUseFilter(String sid, DynamicOneColumnData res, int fetchSize
//            , SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
//        ValueReader valueReader = valueReaders.get(sid);
//        return valueReader.readOneColumnUseFilter(res, fetchSize, timeFilter, freqFilter, valueFilter);
//    }
//
//    public DynamicOneColumnData readOneColumn(String sid, DynamicOneColumnData res, int fetchSize) throws IOException {
//        ValueReader valueReader = valueReaders.get(sid);
//        return valueReader.readOneColumn(res, fetchSize);
//    }
//
//    public ValueReader getValueReaderForSpecificMeasurement(String sid) {
//        return getValueReaders().get(sid);
//    }
//
//    public long getTotalByteSize() {
//        return totalByteSize;
//    }
//
//    public void setTotalByteSize(long totalByteSize) {
//        this.totalByteSize = totalByteSize;
//    }
//
//    @Override
//    public Map<String, ValueReader> getValueReaders() {
//        return valueReaders;
//    }
//
//    public void setValueReaders(HashMap<String, ValueReader> valueReaders) {
//        this.valueReaders = valueReaders;
//    }
//
//    public ITsRandomAccessFileReader getRaf() {
//        return raf;
//    }
//
//    public void setRaf(ITsRandomAccessFileReader raf) {
//        this.raf = raf;
//    }
//
//}
