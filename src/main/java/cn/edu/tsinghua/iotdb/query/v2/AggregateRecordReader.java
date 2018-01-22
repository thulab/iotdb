package cn.edu.tsinghua.iotdb.query.v2;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.reader.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.reader.ReaderUtils;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.iotdb.query.reader.UpdateOperation;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class AggregateRecordReader extends RecordReader{

    private static final Logger logger = LoggerFactory.getLogger(AggregateRecordReader.class);

    public AggregateRecordReader(List<String> filePathList, String deltaObjectId, String measurementId, DynamicOneColumnData lastPageInMemory,
                                 List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName, List<Object> overflowInfo) throws PathErrorException {
        super(filePathList, deltaObjectId, measurementId, lastPageInMemory, bufferWritePageList, compressionTypeName, overflowInfo);
    }

    public AggregateRecordReader(List<String> filePathList, String unsealedFilePath, List<RowGroupMetaData> rowGroupMetadataList,
                                 String deltaObjectId, String measurementId, DynamicOneColumnData lastPageInMemory,
                                 List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName, List<Object> overflowInfo) throws PathErrorException {
        super(filePathList, unsealedFilePath, rowGroupMetadataList, deltaObjectId, measurementId, lastPageInMemory, bufferWritePageList, compressionTypeName, overflowInfo);
    }

    /**
     * Aggregation calculate function of <code>RecordReader</code> without filter.
     *
     * @param aggregateFunction aggregation function
     * @param queryTimeFilter time filter
     * @param valueFilter value filter
     * @return aggregation result
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public AggregateFunction aggregate(AggregateFunction aggregateFunction,
                                       SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression valueFilter
    ) throws ProcessorException, IOException {

        SingleSeriesFilterExpression mergeTimeFilter = mergeTimeFilter(queryTimeFilter, overflowTimeFilter);

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, mergeTimeFilter);

        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {
                aggregate(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, overflowUpdateOperation, mergeTimeFilter, valueFilter);
            }
        }

        // consider left insert values
        // all timestamp of these values are greater than timestamp in List<RowGroupReader>
        if (insertMemoryData != null && insertMemoryData.hasInsertData()) {
            aggregateFunction.calculateValueFromLeftMemoryData(insertMemoryData);
        }

        return aggregateFunction;
    }

    /**
     * <p>
     * Calculate the aggregate result using the given timestamps.
     * Return a pair of AggregationResult and Boolean, AggregationResult represents the aggregation result,
     * Boolean represents that whether there still has unread data.
     *
     * @param aggregateFunction aggregation function
     * @param queryTimeFilter time filter
     * @param timestamps timestamps calculated by the cross filter
     * @return aggregation result and whether still has unread data
     * @throws ProcessorException aggregation invoking exception
     * @throws IOException TsFile read exception
     */
    public Pair<AggregateFunction, Boolean> aggregateUsingTimestamps(AggregateFunction aggregateFunction, SingleSeriesFilterExpression queryTimeFilter,
                                                                     List<Long> timestamps) throws ProcessorException, IOException {
        boolean stillHasUnReadData;

        List<RowGroupReader> rowGroupReaderList = tsFileReaderManager.getRowGroupReaderListByDeltaObject(deltaObjectId, overflowTimeFilter);

        int commonTimestampsIndex = 0;

        int rowGroupIndex = aggregateFunction.resultData.rowGroupIndex;

        for (; rowGroupIndex < rowGroupReaderList.size(); rowGroupIndex++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(rowGroupIndex);
            if (rowGroupReader.getValueReaders().containsKey(measurementId) &&
                    rowGroupReader.getValueReaders().get(measurementId).getDataType().equals(dataType)) {

                // TODO commonTimestampsIndex could be saved as a parameter

                commonTimestampsIndex = aggregateUsingTimestamps(rowGroupReader.getValueReaders().get(measurementId),
                        aggregateFunction, overflowUpdateOperation, overflowTimeFilter, timestamps);

                // all value of commonTimestampsIndex has been used,
                // the next batch of commonTimestamps should be loaded
                if (commonTimestampsIndex >= timestamps.size()) {
                    return new Pair<>(aggregateFunction, true);
                }
            }
        }

        // calculate aggregation using unsealed file data and memory data
        if (insertMemoryData.hasInsertData()) {
            stillHasUnReadData = aggregateFunction.calcAggregationUsingTimestamps(insertMemoryData, timestamps, commonTimestampsIndex);
        } else {
            if (commonTimestampsIndex < timestamps.size()) {
                stillHasUnReadData = false;
            } else {
                stillHasUnReadData = true;
            }
        }

        return new Pair<>(aggregateFunction, stillHasUnReadData);
    }

    private void aggregate(ValueReader valueReader, AggregateFunction func, UpdateOperation updateOperation,
                           SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter)
            throws IOException, ProcessorException {

        TSDataType dataType = valueReader.dataType;
        DynamicOneColumnData result = new DynamicOneColumnData(dataType, true);
        result.pageOffset = valueReader.fileOffset;

        // get series digest
        TsDigest digest = valueReader.getDigest();
        DigestForFilter valueDigest = new DigestForFilter(digest.getStatistics().get(AggregationConstant.MIN_VALUE),
                digest.getStatistics().get(AggregationConstant.MAX_VALUE), dataType);
        logger.debug("calculate aggregation without filter : series digest min and max is: "
                + valueDigest.getMinValue() + " --- " + valueDigest.getMaxValue());
        DigestVisitor valueDigestVisitor = new DigestVisitor();

        // skip the current series chunk according to time filter
        IntervalTimeVisitor seriesTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !seriesTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            logger.debug("series time digest does not satisfy time filter");
            result.plusRowGroupIndexAndInitPageOffset();
        }

        // skip the current series chunk according to value filter
        if (valueFilter != null && !valueDigestVisitor.satisfy(valueDigest, valueFilter)) {
            if ((!updateOperation.hasNext() || updateOperation.getUpdateStartTime() > valueReader.getEndTime()) &&
                    (!insertMemoryData.hasInsertData() || insertMemoryData.getCurrentMinTime() > valueReader.getEndTime())) {
                logger.debug("series value digest does not satisfy value filter");
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
     * @param overflowTimeFilter time filter
     * @param aggregationTimestamps the timestamps which aggregation must satisfy
     * @return an int value, represents the read time index of timestamps
     * @throws IOException TsFile read error
     * @throws ProcessorException get read info error
     */
    private int aggregateUsingTimestamps(ValueReader valueReader, AggregateFunction aggregateFunction, UpdateOperation updateOperation,
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
        logger.debug("calculate aggregation using given common timestamps, series Digest min and max is: "
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

    private boolean checkDataChanged(long pageMinTime, long pageMaxTime, UpdateOperation updateOperation, InsertDynamicData insertMemoryData)
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
