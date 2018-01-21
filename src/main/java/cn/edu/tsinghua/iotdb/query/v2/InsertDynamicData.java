package cn.edu.tsinghua.iotdb.query.v2;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.reader.InsertOperation;
import cn.edu.tsinghua.iotdb.query.reader.UpdateOperation;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.encoding.decoder.DeltaBinaryDecoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iotdb.query.reader.ReaderUtils.getSingleValueVisitorByDataType;

/**
 * InsertDynamicData is encapsulating class for page list, last page and overflow data.
 * A hasNext and removeCurrentValue method is recommended.
 *
 * @author CGF
 */
public class InsertDynamicData {

    private static final Logger LOG = LoggerFactory.getLogger(InsertDynamicData.class);
    private TSDataType dataType;
    private CompressionTypeName compressionTypeName;
    private boolean hasNext = false;

    /** unsealed page list **/
    private List<ByteArrayInputStream> pageList;

    /** used page index of pageList **/
    private int pageIndex = 0;

    /** page reader **/
    private PageReader pageReader = null;

    /** value inputstream for current read page, this variable is not null **/
    private InputStream page = null;

    /** time for current read page **/
    private long[] pageTimes;

    /** used time index for pageTimes**/
    private int pageTimeIndex = -1;

    /** last page data in memory **/
    private InsertOperation lastPageData;

    /** overflow insert data, this variable is not null **/
    private InsertOperation overflowInsertData;

    /** overflow update data which is satisfied with filter, this variable is not null **/
    private UpdateOperation overflowUpdateOperation;

    /** time decoder **/
    private Decoder timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();

    /** value decoder **/
    private Decoder valueDecoder;

    /** current satisfied time **/
    private long currentSatisfiedTime = -1;

    /** time filter for this series **/
    public SingleSeriesFilterExpression timeFilter;

    /** value filter for this series **/
    public SingleSeriesFilterExpression valueFilter;

    /** IntervalTimeVisitor for page time digest **/
    private IntervalTimeVisitor intervalTimeVisitor = new IntervalTimeVisitor();

    private int curSatisfiedIntValue;
    private int[] pageIntValues;
    private boolean curSatisfiedBooleanValue;
    private boolean[] pageBooleanValues;
    private long curSatisfiedLongValue;
    private long[] pageLongValues;
    private float curSatisfiedFloatValue;
    private float[] pageFloatValues;
    private double curSatisfiedDoubleValue;
    private double[] pageDoubleValues;
    private Binary curSatisfiedBinaryValue;
    private Binary[] pageBinaryValues;

    private DigestVisitor digestVisitor = new DigestVisitor();
    private SingleValueVisitor singleValueVisitor;
    private SingleValueVisitor singleTimeVisitor;

    public InsertDynamicData(TSDataType dataType, CompressionTypeName compressionName,
                             List<ByteArrayInputStream> pageList, DynamicOneColumnData lastPageData,
                             DynamicOneColumnData overflowInsertData, DynamicOneColumnData overflowUpdateOperation,
                             SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter) {
        this.dataType = dataType;
        this.compressionTypeName = compressionName;

        this.pageList = pageList == null ? new ArrayList<>() : pageList;
        this.lastPageData = new InsertOperation(dataType, lastPageData);

        this.overflowInsertData = new InsertOperation(dataType, overflowInsertData);
        this.overflowUpdateOperation = new UpdateOperation(dataType, overflowUpdateOperation, valueFilter);

        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        this.singleValueVisitor = getSingleValueVisitorByDataType(dataType, valueFilter);
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public long getCurrentMinTime() {
        return currentSatisfiedTime;
    }

    public int getCurrentIntValue() {
        return curSatisfiedIntValue;
    }

    public boolean getCurrentBooleanValue() {
        return curSatisfiedBooleanValue;
    }

    public long getCurrentLongValue() {
        return curSatisfiedLongValue;
    }

    public float getCurrentFloatValue() {
        return curSatisfiedFloatValue;
    }

    public double getCurrentDoubleValue() {
        return curSatisfiedDoubleValue;
    }

    public Binary getCurrentBinaryValue() {
        return curSatisfiedBinaryValue;
    }

    public Object getCurrentObjectValue() {
        switch (dataType) {
            case INT32:
                return getCurrentIntValue();
            case INT64:
                return getCurrentLongValue();
            case BOOLEAN:
                return getCurrentBooleanValue();
            case FLOAT:
                return getCurrentFloatValue();
            case DOUBLE:
                return getCurrentDoubleValue();
            case TEXT:
                return getCurrentBinaryValue();
            default:
                throw new UnSupportedDataTypeException("UnSupported aggregation datatype: " + dataType);
        }
    }

    public void removeCurrentValue() {
        hasNext = false;
    }

    public boolean hasInsertData() throws IOException {
        if (hasNext)
            return true;

        if (pageIndex < pageList.size()) {
            hasNext = readPageList();
            if (hasNext)
                return true;
        }

        hasNext = readLastPage();
        if (hasNext)
            return true;

        while (overflowInsertData.hasNext()) {
            if (examineOverflowInsert()) {
                overflowInsertData.next();
                hasNext = true;
                return true;
            } else {
                overflowInsertData.next();
            }
        }

        return false;
    }

    private boolean readPageList() throws IOException {
        while (pageIndex < pageList.size()) {
            if (pageTimes != null) {
                boolean getNext = getSatisfiedTimeAndValue();
                if (getNext) {
                    return true;
                } else {
                    pageReaderReset();
                }
            }

            if (pageIndex >= pageList.size()) {
                return false;
            }

            // construct time and value digest
            pageReader = new PageReader(pageList.get(pageIndex), compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigest = new DigestForFilter(pageDigest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    pageDigest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigest = new DigestForFilter(mint, maxt);
            LOG.debug("Page min time:{}, max time:{}, min value:{}, max value:{}",
                    String.valueOf(mint), String.valueOf(maxt),
                    pageDigest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    pageDigest.getStatistics().get(AggregationConstant.MAX_VALUE));

            while (overflowUpdateOperation.hasNext() && overflowUpdateOperation.getUpdateEndTime() < mint) {
                overflowUpdateOperation.next();
            }

            // not satisfied with time filter.
            if (!digestVisitor.satisfy(timeDigest, timeFilter)) {
                pageReaderReset();
                continue;
            } else {
                if (!digestVisitor.satisfy(valueDigest, valueFilter)) {
                    if (!overflowUpdateOperation.hasNext()
                            || (overflowUpdateOperation.hasNext() && overflowUpdateOperation.getUpdateStartTime() > maxt)
                            || (overflowUpdateOperation.hasNext() && overflowUpdateOperation.getUpdateStartTime() >= mint
                                    && overflowUpdateOperation.getUpdateEndTime() <= maxt && !overflowUpdateOperation.verifyValue())) {
                        pageReaderReset();
                        continue;
                    }
                }
            }

            getCurrentPageTimeAndValues(pageHeader);
        }

        return false;
    }

    private boolean getSatisfiedTimeAndValue() {
        if (pageTimeIndex >= pageTimes.length) {
            return false;
        }

        while (pageTimeIndex < pageTimes.length) {

            // get all the overflow insert time which is less than page time
            while (overflowInsertData.hasNext() && overflowInsertData.getInsertTime() <= pageTimes[pageTimeIndex]) {
                if (overflowInsertData.getInsertTime() < pageTimes[pageTimeIndex]) {
                    if (examineOverflowInsert()) {
                        overflowInsertData.next();
                        return true;
                    } else {
                        overflowInsertData.next();
                    }
                } else {
                    // overflow insert time equals to page time
                    if (examineOverflowInsert()) {
                        overflowInsertData.next();
                        pageTimeIndex ++;
                        return true;
                    } else {
                        overflowInsertData.next();
                        pageTimeIndex ++;
                    }
                }
            }

            while (pageTimeIndex < pageTimes.length && timeFilter != null && !singleTimeVisitor.verify(pageTimes[pageTimeIndex])) {
                pageTimeIndex++;
            }

            if (pageTimeIndex >= pageTimes.length) {
                pageReaderReset();
                return false;
            }

            if (examinePageValue()) {
                pageTimeIndex ++;
                return true;
            } else {
                pageTimeIndex ++;
            }
        }

        return false;
    }

    private boolean examineOverflowInsert() {

        //In current overflow implementation version, overflow insert data must be satisfied with time filter.
        //data may be inserted after deleting.

        //if (overflowTimeFilter != null && singleTimeVisitor.verifyTime(overflowInsertData.getInsertTime()))
        //    return false;

        long time = overflowInsertData.getInsertTime();

        // this examination is important
        // after this examination, the update value must be satisfied with the value filter
        if (overflowUpdateOperation.verifyTime(time) && !overflowUpdateOperation.verifyValue()) {
            return false;
        }

        switch (dataType) {
            case INT32:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = overflowUpdateOperation.getInt();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getInt(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = overflowInsertData.getInt();
                    return true;
                }
                return false;
            case INT64:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedLongValue = overflowUpdateOperation.getLong();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getLong(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedLongValue = overflowInsertData.getLong();
                    return true;
                }
                return false;
            case FLOAT:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedFloatValue = overflowUpdateOperation.getFloat();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getFloat(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedFloatValue = overflowInsertData.getFloat();
                    return true;
                }
                return false;
            case DOUBLE:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedDoubleValue = overflowUpdateOperation.getDouble();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getDouble(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedDoubleValue = overflowInsertData.getDouble();
                    return true;
                }
                return false;
            case TEXT:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedBinaryValue = overflowUpdateOperation.getText();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getText(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBinaryValue = overflowInsertData.getText();
                    return true;
                }
                return false;
            case BOOLEAN:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = overflowUpdateOperation.getInt();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getBoolean(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBooleanValue = overflowInsertData.getBoolean();
                    return true;
                }
                return false;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    private boolean examinePageValue() {
        long time = pageTimes[pageTimeIndex];
        if (timeFilter != null && !singleTimeVisitor.verify(time))
            return false;

        while (overflowUpdateOperation.hasNext() && overflowUpdateOperation.getUpdateEndTime() < time)
            overflowUpdateOperation.next();

        // this examination is important
        // after this examination, the update value must be satisfied with the value filter
        if (overflowUpdateOperation.verifyTime(time) && !overflowUpdateOperation.verifyValue()) {
            return false;
        }

        switch (dataType) {
            case INT32:
                if (overflowUpdateOperation.verifyTime(time)) {
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = overflowUpdateOperation.getInt();
                    return false;
                }
                if (singleValueVisitor.satisfyObject(pageIntValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = pageIntValues[pageTimeIndex];
                    return true;
                }
                return false;
            case INT64:
                if (overflowUpdateOperation.verifyTime(time)) {
                    currentSatisfiedTime = time;
                    curSatisfiedLongValue = overflowUpdateOperation.getLong();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(pageLongValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                    return true;
                }
                return false;
            case FLOAT:
                if (overflowUpdateOperation.verifyTime(time)) {
                    currentSatisfiedTime = time;
                    curSatisfiedFloatValue = overflowUpdateOperation.getFloat();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(pageFloatValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedFloatValue = pageFloatValues[pageTimeIndex];
                    return true;
                }
                return false;
            case DOUBLE:
                if (overflowUpdateOperation.verifyTime(time)) {
                    currentSatisfiedTime = time;
                    curSatisfiedDoubleValue = overflowUpdateOperation.getDouble();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(pageDoubleValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedDoubleValue = pageDoubleValues[pageTimeIndex];
                    return true;
                }
                return false;
            case TEXT:
                if (overflowUpdateOperation.verifyTime(time)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBinaryValue = overflowUpdateOperation.getText();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(pageBinaryValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBinaryValue = pageBinaryValues[pageTimeIndex];
                    return true;
                }
                return false;
            case BOOLEAN:
                if (overflowUpdateOperation.verifyTime(time)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBooleanValue = overflowUpdateOperation.getBoolean();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(pageBooleanValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBooleanValue = pageBooleanValues[pageTimeIndex];
                    return true;
                }
                return false;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    private void getCurrentPageTimeAndValues(PageHeader pageHeader) throws IOException {
        page = pageReader.getNextPage();
        pageTimes = initTimeValue(page, pageHeader.data_page_header.num_rows);
        pageTimeIndex = 0;

        valueDecoder = Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType);
        int cnt = 0;
        switch (dataType) {
            case INT32:
                pageIntValues = new int[pageHeader.data_page_header.num_rows];
                while (valueDecoder.hasNext(page)) {
                    pageIntValues[cnt++] = valueDecoder.readInt(page);
                }
                break;
            case INT64:
                pageLongValues = new long[pageHeader.data_page_header.num_rows];
                while (valueDecoder.hasNext(page)) {
                    pageLongValues[cnt++] = valueDecoder.readLong(page);
                }
                break;
            case FLOAT:
                pageFloatValues = new float[pageHeader.data_page_header.num_rows];
                while (valueDecoder.hasNext(page)) {
                    pageFloatValues[cnt++] = valueDecoder.readFloat(page);
                }
                break;
            case DOUBLE:
                pageDoubleValues = new double[pageHeader.data_page_header.num_rows];
                while (valueDecoder.hasNext(page)) {
                    pageDoubleValues[cnt++] = valueDecoder.readDouble(page);
                }
                break;
            case BOOLEAN:
                pageBooleanValues = new boolean[pageHeader.data_page_header.num_rows];
                while (valueDecoder.hasNext(page)) {
                    pageBooleanValues[cnt++] = valueDecoder.readBoolean(page);
                }
                break;
            case TEXT:
                pageBinaryValues = new Binary[pageHeader.data_page_header.num_rows];
                while (valueDecoder.hasNext(page)) {
                    pageBinaryValues[cnt++] = valueDecoder.readBinary(page);
                }
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    private boolean readLastPage() {

        // get all the overflow insert time which is less than page time
        while (lastPageData.hasNext()) {
            while (overflowInsertData.hasNext() && overflowInsertData.getInsertTime() <= lastPageData.getInsertTime()) {
                if (overflowInsertData.getInsertTime() < lastPageData.getInsertTime()) {
                    if (examineOverflowInsert()) {
                        overflowInsertData.next();
                        return true;
                    } else {
                        overflowInsertData.next();
                    }
                } else {
                    // overflow insert time equals to page time
                    if (examineOverflowInsert()) {
                        overflowInsertData.next();
                        lastPageData.next();
                        return true;
                    } else {
                        lastPageData.next();
                        overflowInsertData.next();
                    }
                }
            }

            if (lastPageData.hasNext()) {
                if (examineLastPage()) {
                    lastPageData.next();
                    return true;
                } else {
                    lastPageData.next();
                }
            }
        }

        return false;
    }

    private boolean examineLastPage() {
        long time = lastPageData.getInsertTime();

        if (timeFilter != null && !singleTimeVisitor.verify(time))
            return false;

        while (overflowUpdateOperation.hasNext() && overflowUpdateOperation.getUpdateEndTime() < time)
            overflowUpdateOperation.next();

        // this examination is important
        // after this examination, the update value must be satisfied with the value filter
        if (overflowUpdateOperation.verifyTime(time) && !overflowUpdateOperation.verifyValue()) {
            return false;
        }

        switch (dataType) {
            case INT32:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = overflowUpdateOperation.getInt();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(lastPageData.getInt(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedIntValue = lastPageData.getInt();
                    return true;
                }
                return false;
            case INT64:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedLongValue = overflowUpdateOperation.getLong();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(lastPageData.getLong(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedLongValue = lastPageData.getLong();
                    return true;
                }
                return false;
            case FLOAT:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedFloatValue = overflowUpdateOperation.getFloat();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(lastPageData.getFloat(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedFloatValue = lastPageData.getFloat();
                    return true;
                }
                return false;
            case DOUBLE:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedDoubleValue = overflowUpdateOperation.getDouble();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(lastPageData.getDouble(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedDoubleValue = lastPageData.getDouble();
                    return true;
                }
                return false;
            case TEXT:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedBinaryValue = overflowUpdateOperation.getText();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(lastPageData.getText(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBinaryValue = lastPageData.getText();
                    return true;
                }
                return false;
            case BOOLEAN:
                if (overflowUpdateOperation.verifyTime(time)){
                    currentSatisfiedTime = time;
                    curSatisfiedBooleanValue = overflowUpdateOperation.getBoolean();
                    return true;
                }
                if (singleValueVisitor.satisfyObject(lastPageData.getBoolean(), valueFilter)) {
                    currentSatisfiedTime = time;
                    curSatisfiedBooleanValue = lastPageData.getBoolean();
                    return true;
                }
                return false;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    private void pageReaderReset() {
        pageIndex++;
        pageReader = null;
        pageTimes = null;
        currentSatisfiedTime = -1;
    }

    /**
     * Read time value from the page and return them.
     *
     * @param page data page input stream
     * @param size data page input stream size
     * @throws IOException read page error
     */
    private long[] initTimeValue(InputStream page, int size) throws IOException {
        long[] res;
        int idx = 0;

        int length = ReadWriteStreamUtils.readUnsignedVarInt(page);
        byte[] buf = new byte[length];
        int readSize = page.read(buf, 0, length);

        ByteArrayInputStream bis = new ByteArrayInputStream(buf);
        res = new long[size];
        while (timeDecoder.hasNext(bis)) {
            res[idx++] = timeDecoder.readLong(bis);
        }

        return res;
    }
}
