package cn.edu.tsinghua.iotdb.query.dataset;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
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
 * A new DynamicOneColumnData which replaces overflowInsertData and contains unsealed PageList.
 *
 * // TODO the structure between page and overflow is not clear
 *
 * @author CGF
 */
public class InsertDynamicDataV2 {

    private static final Logger LOG = LoggerFactory.getLogger(InsertDynamicDataV2.class);
    private TSDataType dataType;
    private CompressionTypeName compressionTypeName;

    /** unsealed page list **/
    private List<ByteArrayInputStream> pageList;
    private int pageIndex = 0;
    private PageReader pageReader = null;
    /** value inputstream for current read page, this variable is not null **/
    private InputStream page = null;
    /** time for current read page **/
    private long[] timeValues;

    private int curTimeIndex = -1;

    /** last page data in memory **/
    private DynamicOneColumnData lastPageData;

    /** overflow insert data, this variable is not null **/
    private DynamicOneColumnData overflowInsertData;

    /** overflow update data which is satisfied with filter, this variable is not null **/
    private DynamicOneColumnData overflowUpdateTrue;

    /** overflow update data which is not satisfied with filter, this variable is not null**/
    private DynamicOneColumnData overflowUpdateFalse;
    
    private Decoder timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder(), valueDecoder;
    private long currentSatisfiedPageTime = -1; // timestamp for page list

    /** time filter for this series **/
    public SingleSeriesFilterExpression timeFilter;
    /** value filter for this series **/
    public SingleSeriesFilterExpression valueFilter;

    private int curSatisfiedIntValue;
    private boolean curSatisfiedBooleanValue;
    private long curSatisfiedLongValue;
    private float curSatisfiedFloatValue;
    private double curSatisfiedDoubleValue;
    private Binary curSatisfiedBinaryValue;

    private DigestVisitor digestVisitor = new DigestVisitor();
    private SingleValueVisitor singleValueVisitor;
    private SingleValueVisitor singleTimeVisitor;

    public InsertDynamicDataV2(TSDataType dataType, CompressionTypeName compressionName,
                               List<ByteArrayInputStream> pageList, DynamicOneColumnData lastPageData,
                               DynamicOneColumnData overflowInsertData, DynamicOneColumnData overflowoverflowUpdateTrue, DynamicOneColumnData overflowoverflowUpdateFalse,
                               SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter) {
        this.dataType = dataType;
        this.compressionTypeName = compressionName;

        this.pageList = pageList == null ? new ArrayList<>() : pageList;
        this.lastPageData = lastPageData == null ? new DynamicOneColumnData(dataType, true) : lastPageData;

        this.overflowInsertData = overflowInsertData == null ? new DynamicOneColumnData(dataType, true) : overflowInsertData;
        this.overflowUpdateTrue = overflowoverflowUpdateTrue == null ? new DynamicOneColumnData(dataType, true) : overflowoverflowUpdateTrue;
        this.overflowUpdateFalse = overflowoverflowUpdateFalse == null ? new DynamicOneColumnData(dataType, true) : overflowoverflowUpdateFalse;

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
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getTime(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getTime(overflowInsertData.curIdx);
        }

        return currentSatisfiedPageTime;
    }

    public int getCurrentIntValue() {

        // will not exist: currentSatisfiedPageTime = -1 (page list has been read all), but overflowInsertData still has unread timestamp
        // insert time is ok
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getInt(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getInt(overflowInsertData.curIdx);
        } else {
            return curSatisfiedIntValue;
        }
    }

    public boolean getCurrentBooleanValue() {
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getBoolean(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getBoolean(overflowInsertData.curIdx);
        } else {
            return curSatisfiedBooleanValue;
        }
    }

    public long getCurrentLongValue() {
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getLong(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getLong(overflowInsertData.curIdx);
        } else {
            return curSatisfiedLongValue;
        }
    }

    public float getCurrentFloatValue() {
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getFloat(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getFloat(overflowInsertData.curIdx);
        } else {
            return curSatisfiedFloatValue;
        }
    }

    public double getCurrentDoubleValue() {
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getDouble(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getDouble(overflowInsertData.curIdx);
        } else {
            return curSatisfiedDoubleValue;
        }
    }

    public Binary getCurrentBinaryValue() {
        if (currentSatisfiedPageTime == -1) {
            return overflowInsertData.getBinary(overflowInsertData.curIdx);
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            return overflowInsertData.getBinary(overflowInsertData.curIdx);
        } else {
            return curSatisfiedBinaryValue;
        }
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

    /**
     * Remove current time and value, to get next time and value satisfied with the filters.
     * Must exist current time and value.
     */
    public void removeCurrentValue() throws IOException {
        if (currentSatisfiedPageTime == -1) {
            overflowInsertData.curIdx++;
        }

        if (overflowInsertData.curIdx < overflowInsertData.valueLength && overflowInsertData.getTime(overflowInsertData.curIdx) <= currentSatisfiedPageTime) {
            if (overflowInsertData.getTime(overflowInsertData.curIdx) < currentSatisfiedPageTime) {
                overflowInsertData.curIdx++;
                return;
            } else {
                overflowInsertData.curIdx++;
            }
        }

        // remove page time
        currentSatisfiedPageTime = -1;
        curTimeIndex++;
        if (timeValues != null && curTimeIndex >= timeValues.length) {
            pageIndex++;
            pageReader = null;
            page = null;
            curTimeIndex = 0;
        }
    }

    private boolean readPageList() {
        return false;
    }

    private boolean readLastPage() {
        return false;
    }

    public boolean hasInsertData() throws IOException {
        boolean hasNext = false;

        if (pageIndex < pageList.size()) {
            hasNext = readPageList();
            if (hasNext)
                return true;
        }

        hasNext = readLastPage();


        if (currentSatisfiedPageTime != -1)
            return true;

        boolean pageFindFlag = false;

        // to get next page which has satisfied data
        while (!pageFindFlag) {
            if (pageList == null || (pageReader == null && pageIndex >= pageList.size()))
                break;

            if (pageReader == null) {
                pageReader = new PageReader(pageList.get(pageIndex), compressionTypeName);
                PageHeader pageHeader = pageReader.getNextPageHeader();
                Digest pageDigest = pageHeader.data_page_header.getDigest();

                // construct value filter digest
                DigestForFilter valueDigest = new StrDigestForFilter(pageDigest.getStatistics().get(AggregationConstant.MIN_VALUE),
                        pageDigest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
                // construct time filter digest
                long mint = pageHeader.data_page_header.min_timestamp;
                long maxt = pageHeader.data_page_header.max_timestamp;
                DigestForFilter timeDigest = new DigestForFilter(mint, maxt);
                LOG.debug("Page min time:{}, max time:{}, min value:{}, max value:{}", String.valueOf(mint),
                        String.valueOf(maxt), pageDigest.getStatistics().get(AggregationConstant.MIN_VALUE), pageDigest.getStatistics().get(AggregationConstant.MAX_VALUE));

                while (overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < mint) {
                    overflowUpdateTrue.curIdx ++;
                }

                while (overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < mint) {
                    overflowUpdateFalse.curIdx ++;
                }

                // not satisfied with time filter.
                if ((timeFilter != null && !digestVisitor.satisfy(timeDigest, timeFilter))) {
                    pageReaderReset();
                    continue;
                } else {
                    // no overflowUpdateTrue and overflowUpdateFalse, not satisfied with valueFilter
                    if (overflowUpdateTrue.curIdx >= overflowUpdateTrue.valueLength && overflowUpdateFalse != null && overflowUpdateFalse.curIdx >= overflowUpdateFalse.valueLength
                            && valueFilter != null && !digestVisitor.satisfy(valueDigest, valueFilter)) {
                        pageReaderReset();
                        continue;
                    }
                    // has overflowUpdateTrue, overflowUpdateTrue not update this page and not satisfied with valueFilter
                    else if (overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) >= maxt &&
                            valueFilter != null && !digestVisitor.satisfy(valueDigest, valueFilter)) {
                        pageReaderReset();
                        continue;
                    }
                    // has overflowUpdateFalse and overflowUpdateFalse update this page all
                    else if (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength &&
                            overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) >= mint && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) <= maxt) {
                        pageReaderReset();
                        continue;
                    }
                }

                page = pageReader.getNextPage();
                timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                curTimeIndex = 0;
                this.valueDecoder = Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType);
            }

            if (pageReader != null && currentSatisfiedPageTime == -1) {

                int unValidTimeCount = 0;

                //TODO consider time filter
                while (timeFilter != null && (curTimeIndex<timeValues.length && !singleTimeVisitor.verify(timeValues[curTimeIndex]))) {
                    curTimeIndex++;
                    unValidTimeCount++;
                }

                // all of remain time data are not satisfied with the time filter.
                if (curTimeIndex == timeValues.length) {
                    pageReader = null; // pageReader reset
                    currentSatisfiedPageTime = -1;
                    pageIndex++;
                    continue;
                }

                int cnt;
                switch (dataType) {
                    case INT32:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedIntValue = valueDecoder.readInt(page);
                                cnt++;
                            }

                            curSatisfiedIntValue = valueDecoder.readInt(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateTrue.curIdx ++;
                                while (overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateFalse.curIdx ++;

                                // overflowUpdateTrue.valueLength*2 - 1
                                if (overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = timeValues[curTimeIndex];
                                    curSatisfiedIntValue = overflowUpdateTrue.getInt(overflowUpdateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedIntValue, valueFilter)) {
                                        currentSatisfiedPageTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedPageTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedPageTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedPageTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case INT64:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedLongValue = valueDecoder.readLong(page);
                                cnt++;
                            }

                            curSatisfiedLongValue = valueDecoder.readLong(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateTrue.curIdx ++;
                                while (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateFalse.curIdx ++;

                                // overflowUpdateTrue.valueLength*2 - 1
                                if (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = timeValues[curTimeIndex];
                                    curSatisfiedLongValue = overflowUpdateTrue.getLong(overflowUpdateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedLongValue, valueFilter)) {
                                        currentSatisfiedPageTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedPageTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedPageTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedPageTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case FLOAT:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedFloatValue = valueDecoder.readFloat(page);
                                cnt++;
                            }

                            curSatisfiedFloatValue = valueDecoder.readFloat(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateTrue.curIdx ++;
                                while (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateFalse.curIdx ++;

                                // overflowUpdateTrue.valueLength*2 - 1
                                if (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = timeValues[curTimeIndex];
                                    curSatisfiedFloatValue = overflowUpdateTrue.getFloat(overflowUpdateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedFloatValue, valueFilter)) {
                                        currentSatisfiedPageTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedPageTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedPageTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedPageTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case DOUBLE:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedDoubleValue = valueDecoder.readDouble(page);
                                cnt++;
                            }

                            curSatisfiedDoubleValue = valueDecoder.readDouble(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateTrue.curIdx ++;
                                while (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateFalse.curIdx ++;

                                // overflowUpdateTrue.valueLength*2 - 1
                                if (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = timeValues[curTimeIndex];
                                    curSatisfiedDoubleValue = overflowUpdateTrue.getDouble(overflowUpdateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedDoubleValue, valueFilter)) {
                                        currentSatisfiedPageTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedPageTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedPageTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedPageTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case BOOLEAN:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedBooleanValue = valueDecoder.readBoolean(page);
                                cnt++;
                            }

                            curSatisfiedBooleanValue = valueDecoder.readBoolean(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateTrue.curIdx ++;
                                while (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateFalse.curIdx ++;

                                // overflowUpdateTrue.valueLength*2 - 1
                                if (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = timeValues[curTimeIndex];
                                    curSatisfiedBooleanValue = overflowUpdateTrue.getBoolean(overflowUpdateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedBooleanValue, valueFilter)) {
                                        currentSatisfiedPageTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedPageTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedPageTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedPageTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    case TEXT:
                        cnt = 0;
                        while (valueDecoder.hasNext(page)) {
                            while (cnt < unValidTimeCount) {
                                curSatisfiedBinaryValue = valueDecoder.readBinary(page);
                                cnt++;
                            }

                            curSatisfiedBinaryValue = valueDecoder.readBinary(page);

                            if (timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) {
                                while (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateTrue.curIdx ++;
                                while (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < timeValues[curTimeIndex])
                                    overflowUpdateFalse.curIdx ++;

                                // overflowUpdateTrue.valueLength*2 - 1
                                if (overflowUpdateTrue != null && overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = timeValues[curTimeIndex];
                                    curSatisfiedBinaryValue = overflowUpdateTrue.getBinary(overflowUpdateTrue.curIdx);
                                    pageFindFlag = true;
                                    break;
                                } else if (overflowUpdateFalse != null && overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= timeValues[curTimeIndex]) {
                                    currentSatisfiedPageTime = -1;
                                    curTimeIndex++;
                                } else {
                                    if (valueFilter == null || singleValueVisitor.satisfyObject(curSatisfiedBinaryValue, valueFilter)) {
                                        currentSatisfiedPageTime = timeValues[curTimeIndex];
                                        pageFindFlag = true;
                                        break;
                                    } else {
                                        currentSatisfiedPageTime = -1;
                                        curTimeIndex++;
                                    }
                                }
                            } else {
                                currentSatisfiedPageTime = -1;
                                curTimeIndex++;
                            }

                            // for removeCurrentValue function pageIndex++
                            if (currentSatisfiedPageTime == -1 && !valueDecoder.hasNext(page)) {
                                pageReaderReset();
                                break;
                            }
                        }
                        break;
                    default:
                        throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
                }
            }
        }

        // overflowInsertData value already satisfy the time filter
        while (overflowInsertData != null && overflowInsertData.curIdx < overflowInsertData.valueLength) {
            while (overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength &&
                    overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2+1) < overflowInsertData.getTime(overflowInsertData.curIdx))
                overflowUpdateTrue.curIdx += 1;
            while (overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength &&
                    overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2+1) < overflowInsertData.getTime(overflowInsertData.curIdx))
                overflowUpdateFalse.curIdx += 1;

            if (overflowUpdateTrue.curIdx < overflowUpdateTrue.valueLength && overflowUpdateTrue.getTime(overflowUpdateTrue.curIdx*2) <= overflowInsertData.getTime(overflowInsertData.curIdx)) {
                // currentSatisfiedPageTime = overflowInsertData.getTime(overflowInsertData.curIdx);
                updateNewValue();
                return true;
            }

            if (overflowUpdateFalse.curIdx < overflowUpdateFalse.valueLength && overflowUpdateFalse.getTime(overflowUpdateFalse.curIdx*2) <= overflowInsertData.getTime(overflowInsertData.curIdx)) {
                overflowInsertData.curIdx ++;
                continue;
            }

            if (valueFilter == null || insertValueSatisfied()) {

                // no page time, or overflow insert time is smaller than page time
//                if (currentSatisfiedPageTime == -1 || overflowInsertData.getTime(overflowInsertData.curIdx) < currentSatisfiedPageTime)
//                    currentSatisfiedPageTime = overflowInsertData.getTime(overflowInsertData.curIdx);

                return true;
            } else {
                overflowInsertData.curIdx++;
            }
        }

        return pageFindFlag;
    }

    private boolean insertValueSatisfied() {
        switch (dataType) {
            case INT32:
                return singleValueVisitor.satisfyObject(overflowInsertData.getInt(overflowInsertData.curIdx), valueFilter);
            case INT64:
                return singleValueVisitor.satisfyObject(overflowInsertData.getLong(overflowInsertData.curIdx), valueFilter);
            case FLOAT:
                return singleValueVisitor.satisfyObject(overflowInsertData.getFloat(overflowInsertData.curIdx), valueFilter);
            case DOUBLE:
                return singleValueVisitor.satisfyObject(overflowInsertData.getDouble(overflowInsertData.curIdx), valueFilter);
            case TEXT:
                return singleValueVisitor.satisfyObject(overflowInsertData.getBinary(overflowInsertData.curIdx), valueFilter);
            case BOOLEAN:
                return singleValueVisitor.satisfyObject(overflowInsertData.getBoolean(overflowInsertData.curIdx), valueFilter);
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
    }

    public void pageReaderReset() {
        pageIndex++;
        pageReader = null;
        currentSatisfiedPageTime = -1;
    }

    private void curTimeReset() {
        currentSatisfiedPageTime = -1;
        curTimeIndex++;
    }

    /**
     * Reset the read status, streaming
     */
    public void readStatusReset() {
        if (pageList != null) {
            for (ByteArrayInputStream stream : pageList) {
                stream.reset();
            }
        }
        if (overflowInsertData != null)
            overflowInsertData.curIdx = 0;
        overflowUpdateTrue.curIdx = 0;
        overflowUpdateFalse.curIdx = 0;
        pageIndex = 0;
        pageReader = null;
        curTimeIndex = 0;
        currentSatisfiedPageTime = -1;
    }

    /**
     * update the value of overflowInsertData used updateData.
     */
    private void updateNewValue() {
        switch (dataType) {
            case INT32:
                curSatisfiedIntValue = overflowUpdateTrue.getInt(overflowUpdateTrue.curIdx);
                overflowInsertData.setInt(overflowInsertData.curIdx, curSatisfiedIntValue);
                break;
            case INT64:
                curSatisfiedLongValue = overflowUpdateTrue.getLong(overflowUpdateTrue.curIdx);
                overflowInsertData.setLong(overflowInsertData.curIdx, curSatisfiedLongValue);
                break;
            case FLOAT:
                curSatisfiedFloatValue = overflowUpdateTrue.getFloat(overflowUpdateTrue.curIdx);
                overflowInsertData.setFloat(overflowInsertData.curIdx, curSatisfiedFloatValue);
                break;
            case DOUBLE:
                curSatisfiedDoubleValue = overflowUpdateTrue.getDouble(overflowUpdateTrue.curIdx);
                overflowInsertData.setDouble(overflowInsertData.curIdx, curSatisfiedDoubleValue);
                break;
            case TEXT:
                curSatisfiedBinaryValue = overflowUpdateTrue.getBinary(overflowUpdateTrue.curIdx);
                overflowInsertData.setBinary(overflowInsertData.curIdx, curSatisfiedBinaryValue);
                break;
            case BOOLEAN:
                curSatisfiedBooleanValue = overflowUpdateTrue.getBoolean(overflowUpdateTrue.curIdx);
                overflowInsertData.setBoolean(overflowInsertData.curIdx, curSatisfiedBooleanValue);
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupport Aggregation DataType:" + dataType);
        }
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
     * @param page data page inputstream
     * @param size data page inputstream size
     * @param skip If skip is true, then return long[] which is null.
     * @throws IOException read page error
     */
    private long[] initTimeValue(InputStream page, int size, boolean skip) throws IOException {
        long[] res = null;
        int idx = 0;

        int length = ReadWriteStreamUtils.readUnsignedVarInt(page);
        byte[] buf = new byte[length];
        int readSize = page.read(buf, 0, length);

        if (!skip) {
            ByteArrayInputStream bis = new ByteArrayInputStream(buf);
            res = new long[size];
            while (timeDecoder.hasNext(bis)) {
                res[idx++] = timeDecoder.readLong(bis);
            }
        }

        return res;
    }
}
