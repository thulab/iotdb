package cn.edu.thu.tsfiledb.query.dataset;

import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.encoding.decoder.Decoder;
import cn.edu.thu.tsfile.encoding.decoder.DeltaBinaryDecoder;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.Digest;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.thu.tsfile.timeseries.read.PageReader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * A new DynamicOneColumnData which replaces insertTrue and contains unsealed PageList.
 *
 * @author CGF
 */
public class InsertDynamicData extends DynamicOneColumnData {
    private List<ByteArrayInputStream> pageList;
    private int pageIndex = 0;
    private PageReader pageReader = null;
    private CompressionTypeName compressionTypeName;
    private TSDataType dataType;
    private Decoder timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder(), valueDecoder, freDecoder;
    private boolean pageSatisfy = false, insertSatisfy = false;
    private long currentPageTime = -1; // timestamp for page list
    public SingleSeriesFilterExpression timeFilter, valueFilter, frequencyFilter;

    private int curPageIntValue;
    private boolean curPageBooleanValue;
    private long curPageLongValue;
    private int curTimeIndex = -1;
    private long[] timeValues; // time for current read page
    private InputStream page = null; // value inputstream for current read page

    private DigestVisitor digestVisitor = new DigestVisitor();
    private SingleValueVisitor singleValueVisitor;
    private SingleValueVisitor singleTimeVisitor;
    private boolean valueFlag = false;

    public InsertDynamicData() {
        super();
    }

    public InsertDynamicData(List<ByteArrayInputStream> list, CompressionTypeName name,
                             SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression frequencyFilter,
                             Decoder timeDecoder, Decoder valueDecoder, Decoder freDecoder) {
        this.pageList = list;
        this.compressionTypeName = name;
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.frequencyFilter = frequencyFilter;
        this.timeDecoder = timeDecoder;
        this.valueDecoder = valueDecoder;
        this.freDecoder = freDecoder;
        this.singleValueVisitor = getSingleValueVisitorByDataType(dataType, valueFilter);
        this.singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
    }

    public boolean hasInsertData() throws IOException {
        return findNext();
    }

    public long getCurrentMinTime() {
        if (currentPageTime == -1 || insertTrue.getTime(insertTrue.insertTrueIndex) <= currentPageTime) {
            return insertTrue.getTime(insertTrue.insertTrueIndex);
        } else {
            return currentPageTime;
        }
    }

    public boolean getCurrentBooleanValue() {
        return curPageBooleanValue;
    }

    public int getCurrentIntValue() {
        if (currentPageTime == -1 || insertTrue.getTime(insertTrue.insertTrueIndex) <= currentPageTime) {
            return insertTrue.getInt(insertTrue.insertTrueIndex);
        } else {
            return curPageIntValue;
        }
    }

//    public long getCurrentLongValue() {
//
//    }
//
//    public float getCurrentFloatValue() {
//
//    }
//
//    public double getCurrentDoubleValue() {
//
//    }
//
//    public Binary getCurrentBinaryValue() {
//
//    }

    public void setPageList(List<ByteArrayInputStream> list) {
        this.pageList = list;
    }

    /**
     * Remove current time and value, to get next time and value satisfied with the filters.
     */
    public void rollCurrentValue() throws IOException {
        if (insertTrue.getLong(insertTrue.insertTrueIndex) < currentPageTime) {
            insertTrue.insertTrueIndex++;
            return;
        }

        int validTimeCount = 0;
        while (!(timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex]))) {
            curTimeIndex++;
            validTimeCount++;
        }

        // all of remain time data are not satisfied with the time filter.
        if (curTimeIndex == timeValues.length) {
            pageReader = null; // pageReader reset
            currentPageTime = -1;
            pageIndex++;
            findNext();
            return;
        }

        switch (dataType) {
            case INT32:
                int cnt = 0;
                while (valueDecoder.hasNext(page)) {
                    while (cnt < validTimeCount) {
                        curPageIntValue = valueDecoder.readInt(page);
                        cnt++;
                    }
                    if (!valueDecoder.hasNext(page)) {
                        pageReader = null; // pageReader reset
                        currentPageTime = -1;
                        pageIndex++;
                        findNext();
                        break;
                    }
                    curPageIntValue = valueDecoder.readInt(page);
                    if ((timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) &&
                            valueFilter == null || singleValueVisitor.verify(curPageIntValue)) {
                        currentPageTime = timeValues[curTimeIndex];
                        break;
                    } else {
                        currentPageTime = -1;
                        curTimeIndex++;
                    }
                }
                break;
            case INT64:
            case INT96:

        }
    }

    /**
     * Only when the current page data has been read completely, this method could be invoked.
     */
    private boolean findNext() throws IOException {
        if (currentPageTime != -1)
            return true;

        while (pageReader == null && pageIndex < pageList.size() && currentPageTime !=-1) {
            pageReader = new PageReader(pageList.get(pageIndex), compressionTypeName);
            pageIndex++;
            PageHeader pageHeader = pageReader.getNextPageHeader();
            Digest pageDigest = pageHeader.data_page_header.getDigest();

            // construct value filter digest
            DigestForFilter valueDigestFF = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);
            // construct time filter digest
            long mint = pageHeader.data_page_header.min_timestamp;
            long maxt = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

            if (timeFilter != null && !digestVisitor.satisfy(timeDigestFF, timeFilter) && valueFilter != null && digestVisitor.satisfy(valueDigestFF, valueFilter)) {
                pageIndex++;
                continue;
            }

            timeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            curTimeIndex = 0;
            this.valueDecoder = Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType);

            int validTimeCount = 0;
            while (!(timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex]))) {
                curTimeIndex++;
                validTimeCount++;
            }

            // all of remain time data are not satisfied with the time filter.
            if (curTimeIndex == timeValues.length) {
                pageReader = null; // pageReader reset
                currentPageTime = -1;
                pageIndex++;
                continue;
            }

            switch (dataType) {
                case INT32:
                    int cnt = 0;
                    while (valueDecoder.hasNext(page)) {
                        while (cnt < validTimeCount) {
                            curPageIntValue = valueDecoder.readInt(page);
                            cnt++;
                        }
                        if (!valueDecoder.hasNext(page)) {
                            pageReader = null; // pageReader reset
                            currentPageTime = -1;
                            pageIndex++;
                            findNext();
                            break;
                        }
                        curPageIntValue = valueDecoder.readInt(page);
                        if ((timeFilter == null || singleTimeVisitor.verify(timeValues[curTimeIndex])) &&
                                valueFilter == null || singleValueVisitor.verify(curPageIntValue)) {
                            currentPageTime = timeValues[curTimeIndex];
                            break;
                        } else {
                            currentPageTime = -1;
                            curTimeIndex++;
                        }
                    }
                    break;
                case INT64:
                case INT96:

            }
        }

        if (currentPageTime != -1)
            return true;

        if (insertTrueIndex < insertTrue.length)
            return true;

        return false;
    }

    /**
     * Read time value from the page and return them.
     *
     * @param page
     * @param size
     * @param skip If skip is true, then return long[] which is null.
     * @throws IOException
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
}
