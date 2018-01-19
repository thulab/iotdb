package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
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
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ValueReaderProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ValueReaderProcessor.class);

    static DynamicOneColumnData getValuesWithOverFlow(ValueReader valueReader, UpdateOperation updateOperation, InsertDynamicData insertMemoryData,
                                                      SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                                      DynamicOneColumnData res, int fetchSize) throws IOException {

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        if (res == null) {
            res = new DynamicOneColumnData(dataType, true);
            res.pageOffset = valueReader.getFileOffset();
            res.leftSize = valueReader.getTotalSize();
            res.insertTrueIndex = 0;
        }

        // new series read
        if (res.pageOffset == -1) {
            res.pageOffset = valueReader.getFileOffset();
        }

        TsDigest digest = valueReader.getDigest();
        DigestForFilter valueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        LOG.debug(String.format("read one series digest normally, time range is [%s,%s], value range is [%s,%s]",
                valueReader.getStartTime(), valueReader.getEndTime(), valueDigest.getMinValue(), valueDigest.getMaxValue()));
        DigestVisitor valueDigestVisitor = new DigestVisitor();

        while (updateOperation.hasNext() && updateOperation.getUpdateEndTime() < valueReader.getStartTime()) {
            updateOperation.next();
        }

        // skip the current series chunk according to time filter
        IntervalTimeVisitor seriesTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            LOG.debug("series time digest does not satisfy time filter");
            res.plusRowGroupIndexAndInitPageOffset();
            return res;
        }

        // skip the current series chunk according to value filter
        if (valueFilter != null && !valueDigestVisitor.satisfy(valueDigest, valueFilter)) {
            if ((!updateOperation.hasNext() || updateOperation.getUpdateStartTime() > valueReader.getEndTime()) &&
                    (!insertMemoryData.hasInsertData() || insertMemoryData.getCurrentMinTime() > valueReader.getEndTime())) {
                LOG.debug("series value digest does not satisfy value filter");
                res.plusRowGroupIndexAndInitPageOffset();
                return res;
            }
        }

        // initial one page from file
        ByteArrayInputStream bis = valueReader.initBAISForOnePage(res.pageOffset);
        PageReader pageReader = new PageReader(bis, compressionTypeName);
        int resCount = res.valueLength - res.curIdx;

        while ((res.pageOffset - valueReader.fileOffset) < valueReader.totalSize && resCount < fetchSize) {
            // to help to record byte size in this process of read.
            int lastAvailable = bis.available();
            PageHeader pageHeader = pageReader.getNextPageHeader();

            // construct value degist
            DigestForFilter pageValueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            while (updateOperation.hasNext() && updateOperation.getUpdateEndTime() < pageMinTime) {
                updateOperation.next();
            }

            // skip the current page according to time filter
            if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, pageMinTime, pageMaxTime)) {
                pageReader.skipCurrentPage();
                res.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // skip the current page according to value filter
            if (valueFilter != null && !valueDigestVisitor.satisfy(pageValueDigest, valueFilter)) {
                if ((!updateOperation.hasNext() || updateOperation.getUpdateStartTime() > pageMaxTime) &&
                        (!insertMemoryData.hasInsertData() || insertMemoryData.getCurrentMinTime() > pageMaxTime)) {
                    pageReader.skipCurrentPage();
                    res.pageOffset += lastAvailable - bis.available();
                    continue;
                }
            }

            InputStream page = pageReader.getNextPage();
            res.pageOffset += lastAvailable - bis.available();
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

            try {
                int timeIdx = 0;
                switch (dataType) {
                    case INT32:
                        int[] pageIntValues = new int[pageTimestamps.length];
                        int cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageIntValues[cnt++] = valueReader.decoder.readInt(page);
                        }

                        // TODO there may return many results
                        for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                            while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                                    && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putInt(insertMemoryData.getCurrentIntValue());
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (updateOperation.verifyTime(pageTimestamps[timeIdx]) && !updateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.verify(pageIntValues[timeIdx]))) {
                                res.putTime(pageTimestamps[timeIdx]);
                                if (updateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    res.putInt(updateOperation.getInt());
                                } else {
                                    res.putInt(pageIntValues[timeIdx]);
                                }
                                resCount++;
                            }
                        }
                        break;
                    case BOOLEAN:
                        boolean[] pageBooleanValues = new boolean[pageTimestamps.length];
                        cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageBooleanValues[cnt++] = valueReader.decoder.readBoolean(page);
                        }

                        // TODO there may return many results
                        for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                            while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                                    && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putBoolean(insertMemoryData.getCurrentBooleanValue());
                                resCount++;

                                if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (updateOperation.verifyTime(pageTimestamps[timeIdx]) && !updateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.satisfyObject(pageBooleanValues[timeIdx], valueFilter))) {
                                res.putTime(pageTimestamps[timeIdx]);
                                if (updateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    res.putBoolean(updateOperation.getBoolean());
                                } else {
                                    res.putBoolean(pageBooleanValues[timeIdx]);
                                }
                                resCount++;
                            }
                        }
                        break;
                    case INT64:
                        long[] pageLongValues = new long[pageTimestamps.length];
                        cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageLongValues[cnt++] = valueReader.decoder.readLong(page);
                        }

                        // TODO there may return many results
                        for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                            while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                                    && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putLong(insertMemoryData.getCurrentLongValue());
                                resCount ++;

                                if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (updateOperation.verifyTime(pageTimestamps[timeIdx]) && !updateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.verify(pageLongValues[timeIdx]))) {
                                res.putTime(pageTimestamps[timeIdx]);
                                if (updateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    res.putLong(updateOperation.getLong());
                                } else {
                                    res.putLong(pageLongValues[timeIdx]);
                                }
                                resCount ++;
                            }
                        }
                        break;
                    case FLOAT:
                        float[] pageFloatValues = new float[pageTimestamps.length];
                        cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageFloatValues[cnt++] = valueReader.decoder.readFloat(page);
                        }

                        // TODO there may return many results
                        for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                            while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                                    && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putFloat(insertMemoryData.getCurrentFloatValue());
                                resCount ++;

                                if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (updateOperation.verifyTime(pageTimestamps[timeIdx]) && !updateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.verify(pageFloatValues[timeIdx]))) {
                                res.putTime(pageTimestamps[timeIdx]);
                                if (updateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    res.putFloat(updateOperation.getFloat());
                                } else {
                                    res.putFloat(pageFloatValues[timeIdx]);
                                }
                                resCount ++;
                            }
                        }
                        break;
                    case DOUBLE:
                        double[] pageDoubleValues = new double[pageTimestamps.length];
                        cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageDoubleValues[cnt++] = valueReader.decoder.readDouble(page);
                        }

                        // TODO there may return many results
                        for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                            while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                                    && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putDouble(insertMemoryData.getCurrentDoubleValue());
                                resCount ++;

                                if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (updateOperation.verifyTime(pageTimestamps[timeIdx]) && !updateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.verify(pageDoubleValues[timeIdx]))) {
                                res.putTime(pageTimestamps[timeIdx]);
                                if (updateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    res.putDouble(updateOperation.getDouble());
                                } else {
                                    res.putDouble(pageDoubleValues[timeIdx]);
                                }
                                resCount ++;
                            }
                        }
                        break;
                    case TEXT:
                        Binary[] pageTextValues = new Binary[pageTimestamps.length];
                        cnt = 0;
                        while (valueReader.decoder.hasNext(page)) {
                            pageTextValues[cnt++] = valueReader.decoder.readBinary(page);
                        }

                        // TODO there may return many results
                        for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                            while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                                    && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                                res.putTime(insertMemoryData.getCurrentMinTime());
                                res.putBinary(insertMemoryData.getCurrentBinaryValue());
                                resCount ++;

                                if (insertMemoryData.getCurrentMinTime() == pageTimestamps[timeIdx]) {
                                    insertMemoryData.removeCurrentValue();
                                    timeIdx++;
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }
                            }
                            if (timeIdx >= pageTimestamps.length)
                                break;

                            if (updateOperation.verifyTime(pageTimestamps[timeIdx]) && !updateOperation.verifyValue()) {
                                continue;
                            }
                            if ((timeFilter == null || singleTimeVisitor.verify(pageTimestamps[timeIdx])) &&
                                    (valueFilter == null || singleValueVisitor.satisfyObject(pageTextValues[timeIdx], valueFilter))) {
                                res.putTime(pageTimestamps[timeIdx]);
                                if (updateOperation.verifyTime(pageTimestamps[timeIdx])) {
                                    res.putBinary(updateOperation.getText());
                                } else {
                                    res.putBinary(pageTextValues[timeIdx]);
                                }
                                resCount ++;
                            }
                        }
                        break;
                    default:
                        throw new IOException("Data type not support. " + dataType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // represents that current series has been read all.
        if ((res.pageOffset - valueReader.fileOffset) >= valueReader.totalSize) {
            res.plusRowGroupIndexAndInitPageOffset();
        }

        return res;
    }

    static void aggregate(ValueReader valueReader, AggregateFunction func, InsertDynamicData insertMemoryData,
                                UpdateOperation updateOperation, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter)
            throws IOException, ProcessorException {

        TSDataType dataType = valueReader.dataType;
        DynamicOneColumnData result = new DynamicOneColumnData(dataType, true);
        result.pageOffset = valueReader.fileOffset;

        // get series digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter valueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        LOG.debug("calculate aggregation without filter : series digest min and max is: "
                + valueDigest.getMinValue() + " --- " + valueDigest.getMaxValue());
        DigestVisitor valueDigestVisitor = new DigestVisitor();

        // skip the current series chunk according to time filter
        IntervalTimeVisitor seriesTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            LOG.debug("series time digest does not satisfy time filter");
            result.plusRowGroupIndexAndInitPageOffset();
        }

        // skip the current series chunk according to value filter
        if (valueFilter != null && !valueDigestVisitor.satisfy(valueDigest, valueFilter)) {
            if ((!updateOperation.hasNext() || updateOperation.getUpdateStartTime() > valueReader.getEndTime()) &&
                    (!insertMemoryData.hasInsertData() || insertMemoryData.getCurrentMinTime() > valueReader.getEndTime())) {
                LOG.debug("series value digest does not satisfy value filter");
                result.plusRowGroupIndexAndInitPageOffset();
            }
        }

        ByteArrayInputStream bis = valueReader.initBAISForOnePage(result.pageOffset);
        PageReader pageReader = new PageReader(bis, valueReader.compressionTypeName);

        while ((result.pageOffset - valueReader.fileOffset) < valueReader.totalSize) {
            int lastAvailable = bis.available();

            PageHeader pageHeader = pageReader.getNextPageHeader();
            DigestForFilter pageValueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                    digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            // skip the current page according to time filter
            if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, pageMinTime, pageMaxTime)) {
                pageReader.skipCurrentPage();
                result.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // skip the current page according to value filter
            if (valueFilter != null && !valueDigestVisitor.satisfy(pageValueDigest, valueFilter)) {
                if ((!updateOperation.hasNext() || updateOperation.getUpdateStartTime() > pageMaxTime) &&
                        (!insertMemoryData.hasInsertData() || insertMemoryData.getCurrentMinTime() > pageMaxTime)) {
                    pageReader.skipCurrentPage();
                    result.pageOffset += lastAvailable - bis.available();
                    continue;
                }
            }

            InputStream page = pageReader.getNextPage();
            result.pageOffset += lastAvailable - bis.available();

            // whether this page is changed by overflow info
            boolean hasOverflowDataInThisPage = checkDataChanged(pageMinTime, pageMaxTime, updateOperation, insertMemoryData);

            // there is no overflow data in this page
            if (!hasOverflowDataInThisPage) {
                func.calculateValueFromPageHeader(pageHeader);
            } else {
                long[] timeValues = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
                valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
                result = ReaderUtils.readOnePage(dataType, timeValues, valueReader.decoder, page, result,
                        timeFilter, valueFilter, insertMemoryData, updateOperation);
                func.calculateValueFromDataPage(result);
                result.clearData();
            }
        }
    }

    /**
     * <p> An aggregation method implementation for the ValueReader aspect.
     * The aggregation will be calculated using the calculated common timestamps.
     *
     * @param aggregateFunction aggregation function
     * @param insertMemoryData bufferwrite memory insert data with overflow operation
     * @param overflowTimeFilter time filter
     * @param aggregationTimestamps the timestamps which aggregation must satisfy
     * @return an int value, represents the read time index of timestamps
     * @throws IOException TsFile read error
     * @throws ProcessorException get read info error
     */
    static int aggregateUsingTimestamps(ValueReader valueReader, AggregateFunction aggregateFunction, InsertDynamicData insertMemoryData,
                                        UpdateOperation updateOperation,
                                        SingleSeriesFilterExpression overflowTimeFilter, List<Long> aggregationTimestamps)
            throws IOException, ProcessorException {

        TSDataType dataType = valueReader.dataType;

        // the used count of aggregationTimestamps,
        // if all the time of aggregationTimestamps has been read, timestampsUsedIndex >= aggregationTimestamps.size()
        int timestampsUsedIndex = 0;

        // lastAggregationResult records some information such as file page offset
        DynamicOneColumnData lastAggregationResult = aggregateFunction.resultData;
        if (lastAggregationResult.pageOffset == -1) {
            lastAggregationResult.pageOffset = valueReader.fileOffset;
        }

        // get column digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter digestFF = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        LOG.debug("calculate aggregation using given common timestamps, series Digest min and max is: "
                + digestFF.getMinValue() + " --- " + digestFF.getMaxValue() + " min, max time is : "
                + valueReader.getStartTime() + "--" + valueReader.getEndTime());

        DigestVisitor digestVisitor = new DigestVisitor();
        ByteArrayInputStream bis = valueReader.initBAISForOnePage(lastAggregationResult.pageOffset);
        PageReader pageReader = new PageReader(bis, valueReader.compressionTypeName);

        // still has unread data
        while ((lastAggregationResult.pageOffset - valueReader.fileOffset) < valueReader.totalSize) {
            int lastAvailable = bis.available();

            PageHeader pageHeader = pageReader.getNextPageHeader();
            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;
            DigestForFilter timeDigestFF = new DigestForFilter(pageMinTime, pageMaxTime);

            // the min value of common timestamps is greater than max time in this series
            if (aggregationTimestamps.get(timestampsUsedIndex) > pageMaxTime) {
                pageReader.skipCurrentPage();
                lastAggregationResult.pageOffset += lastAvailable - bis.available();
                continue;
            }

            // if the current page doesn't satisfy the time filter
            if (overflowTimeFilter != null && !digestVisitor.satisfy(timeDigestFF, overflowTimeFilter))  {
                pageReader.skipCurrentPage();
                lastAggregationResult.pageOffset += lastAvailable - bis.available();
                continue;
            }

            InputStream page = pageReader.getNextPage();
            long[] pageTimestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), valueReader.getDataType()));

            Pair<DynamicOneColumnData, Integer> pageData = ReaderUtils.readOnePage(
                    dataType, pageTimestamps, valueReader.decoder, page,
                    overflowTimeFilter, aggregationTimestamps, timestampsUsedIndex, insertMemoryData, updateOperation);

            if (pageData.left != null && pageData.left.valueLength > 0)
                aggregateFunction.calculateValueFromDataPage(pageData.left);

            timestampsUsedIndex = pageData.right;
            if (timestampsUsedIndex >= aggregationTimestamps.size())
                break;

            // update lastAggregationResult's pageOffset to the start of next page.
            // notice that : when the aggregationTimestamps is used all, but there still have unused page data,
            // in the next read batch process, the current page will be loaded
            lastAggregationResult.pageOffset += lastAvailable - bis.available();
        }

        if (timestampsUsedIndex < aggregationTimestamps.size())
            lastAggregationResult.plusRowGroupIndexAndInitPageOffset();

        return timestampsUsedIndex;
    }

    private static boolean checkDataChanged(long pageMinTime, long pageMaxTime, UpdateOperation updateOperation, InsertDynamicData insertMemoryData)
            throws IOException {

        while (updateOperation.hasNext() && updateOperation.getUpdateEndTime() < pageMinTime)
            updateOperation.next();

        if (updateOperation.hasNext() && updateOperation.getUpdateStartTime() <= pageMaxTime) {
            return true;
        }

        if (insertMemoryData.hasInsertData()) {
            if (pageMinTime <= insertMemoryData.getCurrentMinTime() && insertMemoryData.getCurrentMinTime() <= pageMaxTime) {
                return true;
            }
            if (pageMaxTime < insertMemoryData.getCurrentMinTime()) {
                return false;
            }
            return true;
        }
        return false;
    }
}
