package cn.edu.tsinghua.iotdb.query.dataset;

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
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.StrDigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * InsertData is encapsulating class for page list, last page and overflow data.
 * A hasNext and removeCurrentValue method is recommended.
 *
 * @author CGF
 */
public class InsertData {

    private static final Logger LOG = LoggerFactory.getLogger(InsertData.class);
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
    private UpdateOperation overflowUpdateTrue;

    /** overflow update data which is not satisfied with filter, this variable is not null**/
    private UpdateOperation overflowUpdateFalse;

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

    public InsertData(TSDataType dataType, CompressionTypeName compressionName,
                      List<ByteArrayInputStream> pageList, DynamicOneColumnData lastPageData,
                      DynamicOneColumnData overflowInsertData, DynamicOneColumnData overflowUpdateTrue, DynamicOneColumnData overflowUpdateFalse,
                      SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter) {
        this.dataType = dataType;
        this.compressionTypeName = compressionName;

        this.pageList = pageList == null ? new ArrayList<>() : pageList;
        this.lastPageData = new InsertOperation(dataType, lastPageData);

        this.overflowInsertData = new InsertOperation(dataType, overflowInsertData);
        this.overflowUpdateTrue = new UpdateOperation(dataType, overflowUpdateTrue);
        this.overflowUpdateFalse = new UpdateOperation(dataType, overflowUpdateFalse);

        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        if (timeFilter != null)
            this.singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        if (valueFilter != null)
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

    public void removeCurrentValue() throws IOException {
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

        return false;
    }

    private boolean readPageList() throws IOException {
        while (pageIndex < pageList.size()) {
            if (pageTimes != null) {
                boolean getNext = getSatisfiedTimeAndValue();
                if (getNext)
                    return true;
            }

            // construct time and value digest
            pageReader = new PageReader(pageList.get(pageIndex), compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();
            Digest pageDigest = pageHeader.data_page_header.getDigest();
            DigestForFilter valueDigest = new StrDigestForFilter(pageDigest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    pageDigest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigest = new DigestForFilter(mint, maxt);
            LOG.debug("Page min time:{}, max time:{}, min value:{}, max value:{}",
                    String.valueOf(mint), String.valueOf(maxt),
                    pageDigest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    pageDigest.getStatistics().get(AggregationConstant.MAX_VALUE));

            while (overflowUpdateTrue.hasNext() && overflowUpdateTrue.getUpdateStartTime() < mint) {
                overflowUpdateTrue.next();
            }

            while (overflowUpdateFalse.hasNext() && overflowUpdateFalse.getUpdateStartTime() < mint) {
                overflowUpdateFalse.next();
            }

            // not satisfied with time filter.
            if (!digestVisitor.satisfy(timeDigest, timeFilter)) {
                pageReaderReset();
                continue;
            } else {
                if (!overflowUpdateTrue.hasNext() && !overflowUpdateFalse.hasNext() && !digestVisitor.satisfy(valueDigest, valueFilter)) {
                    // no overflowUpdateTrue and overflowUpdateFalse, not satisfied with value filter
                    pageReaderReset();
                    continue;
                } else if (overflowUpdateTrue.hasNext() && overflowUpdateTrue.getUpdateEndTime() > maxt && !digestVisitor.satisfy(valueDigest, valueFilter)) {
                    // has overflowUpdateTrue, overflowUpdateTrue not update this page and not satisfied with value filter
                    pageReaderReset();
                    continue;
                } else if (overflowUpdateFalse.hasNext() && overflowUpdateFalse.getUpdateStartTime() >= mint && overflowUpdateFalse.getUpdateEndTime() <= maxt) {
                    // has overflowUpdateFalse and overflowUpdateFalse update this page all
                    pageReaderReset();
                    continue;
                }
            }

            getCurrentPageTimeAndValues(pageHeader);
        }

        return false;
    }

    private boolean getSatisfiedTimeAndValue() throws IOException {
        while (pageTimeIndex < pageTimes.length) {

            // get all the overflow insert time which is less than page time
            while (overflowInsertData.hasNext() && overflowInsertData.getInsertTime() <= pageTimes[pageTimeIndex]) {
                if (overflowInsertData.getInsertTime() < pageTimes[pageTimeIndex]) {
                    if (examineOverflowInsert()) {
                        overflowInsertData.next();
                        return true;
                    }
                } else {
                    // overflow insert time equals to page time
                    if (examineOverflowInsert()) {
                        overflowInsertData.next();
                        pageTimeIndex ++;
                        return true;
                    }
                }
            }

            while (timeFilter != null && !singleTimeVisitor.verify(pageTimes[pageTimeIndex])) {
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
        if (timeFilter != null && singleTimeVisitor.verify(overflowInsertData.getInsertTime()))
            return false;

        // TODO
        // Notice that : in later overflow batch read version (insert and update operation are different apart),
        // we must consider the overflow insert value is updated by update operation.
        // updateOverflowInsertValue();

        switch (dataType) {
            case INT32:
                if (singleValueVisitor.satisfyObject(overflowInsertData.getInt(), valueFilter)) {
                    currentSatisfiedTime = overflowInsertData.getInsertTime();
                    curSatisfiedIntValue = overflowInsertData.getInt();
                    return true;
                }
                return false;
            case INT64:
                if (singleValueVisitor.satisfyObject(overflowInsertData.getLong(), valueFilter)) {
                    currentSatisfiedTime = overflowInsertData.getInsertTime();
                    curSatisfiedLongValue = overflowInsertData.getLong();
                    return true;
                }
                return false;
            case FLOAT:
                if (singleValueVisitor.satisfyObject(overflowInsertData.getFloat(), valueFilter)) {
                    currentSatisfiedTime = overflowInsertData.getInsertTime();
                    curSatisfiedFloatValue = overflowInsertData.getFloat();
                    return true;
                }
                return false;
            case DOUBLE:
                if (singleValueVisitor.satisfyObject(overflowInsertData.getDouble(), valueFilter)) {
                    currentSatisfiedTime = overflowInsertData.getInsertTime();
                    curSatisfiedDoubleValue = overflowInsertData.getDouble();
                    return true;
                }
                return false;
            case TEXT:
                if (singleValueVisitor.satisfyObject(overflowInsertData.getText(), valueFilter)) {
                    currentSatisfiedTime = overflowInsertData.getInsertTime();
                    curSatisfiedBinaryValue = overflowInsertData.getText();
                    return true;
                }
                return false;
            case BOOLEAN:
                if (singleValueVisitor.satisfyObject(overflowInsertData.getBoolean(), valueFilter)) {
                    currentSatisfiedTime = overflowInsertData.getInsertTime();
                    curSatisfiedBooleanValue = overflowInsertData.getBoolean();
                    return true;
                }
                return false;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    private boolean examinePageValue() {
        if (timeFilter != null && singleTimeVisitor.verify(pageTimes[pageTimeIndex]))
            return false;

        // TODO
        // Notice that, in later version, there will only exist one update operation
        while (overflowUpdateTrue.getUpdateEndTime() < pageTimes[pageTimeIndex])
            overflowUpdateTrue.next();
        while (overflowUpdateFalse.getUpdateEndTime() < pageTimes[pageTimeIndex])
            overflowUpdateFalse.next();

        switch (dataType) {
            case INT32:
                if (overflowUpdateTrue.hasNext()
                    && overflowUpdateTrue.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateTrue.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedIntValue = pageIntValues[pageTimeIndex];
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateFalse.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(pageIntValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedIntValue = pageIntValues[pageTimeIndex];
                    return true;
                }
                return false;
            case INT64:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateTrue.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateFalse.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getLong(), valueFilter)) {
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                    return true;
                }
                return false;
            case FLOAT:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateTrue.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateFalse.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getFloat(), valueFilter)) {
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedFloatValue = pageFloatValues[pageTimeIndex];
                    return true;
                }
                return false;
            case DOUBLE:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateTrue.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateFalse.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getDouble(), valueFilter)) {
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedDoubleValue = pageDoubleValues[pageTimeIndex];
                    return true;
                }
                return false;
            case TEXT:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateTrue.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateFalse.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getText(), valueFilter)) {
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedBinaryValue = pageBinaryValues[pageTimeIndex];
                    return true;
                }
                return false;
            case BOOLEAN:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateTrue.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
                    curSatisfiedLongValue = pageLongValues[pageTimeIndex];
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=pageTimes[pageTimeIndex] && overflowUpdateFalse.getUpdateEndTime()>=pageTimes[pageTimeIndex]){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getBoolean(), valueFilter)) {
                    currentSatisfiedTime = pageTimes[pageTimeIndex];
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
        if (timeFilter != null && singleTimeVisitor.verify(lastPageData.getInsertTime()))
            return false;

        // TODO
        // Notice that, in later version, there will only exist one update operation
        while (overflowUpdateTrue.getUpdateEndTime() < lastPageData.getInsertTime())
            overflowUpdateTrue.next();
        while (overflowUpdateFalse.getUpdateEndTime() < lastPageData.getInsertTime())
            overflowUpdateFalse.next();

        switch (dataType) {
            case INT32:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateTrue.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedIntValue = lastPageData.getInt();
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateFalse.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(pageIntValues[pageTimeIndex], valueFilter)) {
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedIntValue = lastPageData.getInt();
                    return true;
                }
                return false;
            case INT64:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateTrue.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedLongValue = lastPageData.getLong();
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateFalse.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getLong(), valueFilter)) {
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedLongValue = lastPageData.getLong();
                    return true;
                }
                return false;
            case FLOAT:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateTrue.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedFloatValue = lastPageData.getFloat();
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateFalse.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getFloat(), valueFilter)) {
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedFloatValue = lastPageData.getFloat();
                    return true;
                }
                return false;
            case DOUBLE:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateTrue.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedDoubleValue = lastPageData.getDouble();
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateFalse.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getDouble(), valueFilter)) {
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedDoubleValue = lastPageData.getDouble();
                    return true;
                }
                return false;
            case TEXT:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateTrue.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedBinaryValue = lastPageData.getText();
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateFalse.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getText(), valueFilter)) {
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedBinaryValue = lastPageData.getText();
                    return true;
                }
                return false;
            case BOOLEAN:
                if (overflowUpdateTrue.hasNext()
                        && overflowUpdateTrue.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateTrue.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    currentSatisfiedTime = lastPageData.getInsertTime();
                    curSatisfiedBooleanValue = lastPageData.getBoolean();
                    return true;
                } else if (overflowUpdateFalse.hasNext()
                        && overflowUpdateFalse.getUpdateStartTime()<=lastPageData.getInsertTime() && overflowUpdateFalse.getUpdateEndTime()>=lastPageData.getInsertTime()){
                    return false;
                }
                if (singleValueVisitor.satisfyObject(overflowInsertData.getBoolean(), valueFilter)) {
                    currentSatisfiedTime = lastPageData.getInsertTime();
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
        currentSatisfiedTime = -1;
    }

    private SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
        switch (type) {
            case INT32:
                return new SingleValueVisitor<Integer>(filter);
            case INT64:
                return new SingleValueVisitor<Long>(filter);
            case FLOAT:
                return new SingleValueVisitor<Float>(filter);
            case DOUBLE:
                return new SingleValueVisitor<Double>(filter);
            default:
                return SingleValueVisitorFactory.getSingleValueVisitor(type);
        }
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
