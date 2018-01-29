package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.query.aggregationv2.AggregationConstant;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * <p> This class is used for TsFile batch read.
 * A list of <code>RowGroupReader</code> is needed, the time hasNext method is invoked,
 * the next satisfied value will be calculated.
 * </p>
 *
 * <p>
 * Notice that, this class is not used currently.
 * </p>
 */
public class TsFileBatchReader {

    private static final Logger logger = LoggerFactory.getLogger(TsFileBatchReader.class);

    private int rowGroupReaderIdx;
    private TSDataType dataType;
    private List<RowGroupReader> rowGroupReaderList;
    private boolean hasNext;
    private String measurementId;
    private ValueReader valueReader;
    private PageReader pageReader;
    private SingleSeriesFilterExpression timeFilter, valueFilter;
    private UpdateOperation overflowUpdateOperation;
    private long pageOffset;

    private long currentSatisfiedTime = -1;
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
    private int pageTimeIdx;

    public TsFileBatchReader(List<RowGroupReader> rowGroupReaderList, String measurementId,
                             SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                             UpdateOperation overflowUpdateOperation, TSDataType dataType) {
        this.rowGroupReaderList = rowGroupReaderList;
        this.measurementId = measurementId;
        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.overflowUpdateOperation = overflowUpdateOperation;
        this.dataType = dataType;
        this.rowGroupReaderIdx = 0;
    }

    public boolean hasNext() throws IOException {
        if (hasNext)
            return true;

        while (valueReader != null || rowGroupReaderIdx < rowGroupReaderList.size()) {

            if (valueReader == null) {
                while (rowGroupReaderIdx < rowGroupReaderList.size()) {
                    RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupReaderIdx);
                    if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                            rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                        valueReader = rowGroupReader.getValueReaders().get(measurementId);
                        break;
                    } else {
                        rowGroupReaderIdx ++;
                    }
                }

                if (rowGroupReaderIdx >= rowGroupReaderList.size()) {
                    return false;
                }

                pageOffset = valueReader.getFileOffset();
                CompressionTypeName compressionTypeName = valueReader.compressionTypeName;
                TsDigest digest = valueReader.getDigest();
                DigestForFilter valueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                        digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
                logger.debug(String.format("read one series digest normally, time range is [%s,%s], value range is [%s,%s]",
                        valueReader.getStartTime(), valueReader.getEndTime(), valueDigest.getMinValue(), valueDigest.getMaxValue()));
                DigestVisitor valueDigestVisitor = new DigestVisitor();

                while (overflowUpdateOperation.hasNext() && overflowUpdateOperation.getUpdateEndTime() < valueReader.getStartTime()) {
                    overflowUpdateOperation.next();
                }

                // skip the current series chunk according to time filter
                IntervalTimeVisitor seriesTimeVisitor = new IntervalTimeVisitor();
                if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
                    logger.debug("series time digest does not satisfy time filter");
                    rowGroupReaderIdx ++;
                    valueReaderReset();
                    continue;
                }

                // skip the current series chunk according to value filter
                if (valueFilter != null && !valueDigestVisitor.satisfy(valueDigest, valueFilter)) {
                    if ((!overflowUpdateOperation.hasNext() || overflowUpdateOperation.getUpdateStartTime() > valueReader.getEndTime())) {
                        logger.debug("series value digest does not satisfy value filter");
                        rowGroupReaderIdx ++;
                        valueReaderReset();
                        continue;
                    }
                }


            }

            CompressionTypeName compressionTypeName = valueReader.compressionTypeName;
            ByteArrayInputStream bis = valueReader.initBAISForOnePage(pageOffset);
            PageReader pageReader = new PageReader(bis, compressionTypeName);
            int lastAvailable = bis.available();

            if (pageReader == null) {
                InputStream page = pageReader.getNextPage();
                pageOffset += lastAvailable - bis.available();
                PageHeader pageHeader = pageReader.getNextPageHeader();
                long[] pageTimestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));

                SingleValueVisitor<?> singleTimeVisitor = null;
                if (timeFilter != null) {
                    singleTimeVisitor = valueReader.getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
                }
                SingleValueVisitor<?> singleValueVisitor = null;
                if (valueFilter != null) {
                    singleValueVisitor = valueReader.getSingleValueVisitorByDataType(dataType, valueFilter);
                }

                int timeIdx = 0;
                switch (dataType) {
                    case INT32:
                        int[] pageIntValues = new int[pageTimestamps.length];
                        int cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageIntValues[cnt++] = valueReader.decoder.readInt(page);
                        }

                        for (; timeIdx < pageTimestamps.length; timeIdx++) {

                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (overflowUpdateOperation.verifyTime(pageTimestamps[timeIdx]) && !overflowUpdateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.verify(pageIntValues[timeIdx]))) {
                                currentSatisfiedTime = pageTimestamps[timeIdx];
                                if (overflowUpdateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    curSatisfiedIntValue = overflowUpdateOperation.getInt();
                                } else {
                                    curSatisfiedIntValue = pageIntValues[timeIdx];
                                }
                            }
                        }
                        break;
                }
            }
        }

        return false;
    }

    private void valueReaderReset() {
        valueReader = null;
        pageReader = null;
        currentSatisfiedTime = -1;
    }

    private void pageReaderReset() {
        pageReader = null;
        currentSatisfiedTime = -1;
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
}
