package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


/**
 * This class is a complement of <code>ValueReaderProcessor</code>.
 * The main method in this class is <code>readOnePage</code>, it supplies a page level read logic.
 *
 */
public class ReaderUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReaderUtils.class);

    public static SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
        if (filter == null) {
            return new SingleValueVisitor<>();
        }

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
     * <p> Read one page data,
     * this page data may be changed by overflow operation, so the overflow parameter is required.
     * This method is only used for aggregation function.
     *
     * @param dataType the <code>DataType</code> of the read page
     * @param pageTimestamps the decompressed timestamps of this page
     * @param decoder the <code>Decoder</code> of current page
     * @param page Page data
     * @param res same as result data, we need pass it many times
     * @param timeFilter time filter
     * @param valueFilter value filter
     * @param insertMemoryData the memory data(bufferwrite along with overflow)
     * @return DynamicOneColumnData of the read result
     * @throws IOException TsFile read error
     */
    public static DynamicOneColumnData readOnePage(TSDataType dataType, long[] pageTimestamps,
                                                   Decoder decoder, InputStream page, DynamicOneColumnData res,
                                                   SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                                   InsertDynamicData insertMemoryData, UpdateOperation updateOperation) throws IOException {
        SingleValueVisitor<?> singleTimeVisitor = null;
        if (timeFilter != null) {
            singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        }
        SingleValueVisitor<?> singleValueVisitor = null;
        if (valueFilter != null) {
            singleValueVisitor = getSingleValueVisitorByDataType(dataType, valueFilter);
        }

        int timeIdx = 0;
        switch (dataType) {
            case INT32:
                int[] pageIntValues = new int[pageTimestamps.length];
                int cnt = 0;
                while (decoder.hasNext(page)) {
                    pageIntValues[cnt++] = decoder.readInt(page);
                }

                // TODO there may return many results
                for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                    while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        res.putInt(insertMemoryData.getCurrentIntValue());

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
                    }
                }
                break;
            case BOOLEAN:
                boolean[] pageBooleanValues = new boolean[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageBooleanValues[cnt++] = decoder.readBoolean(page);
                }

                // TODO there may return many results
                for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                    while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        res.putBoolean(insertMemoryData.getCurrentBooleanValue());

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
                    }
                }
                break;
            case INT64:
                long[] pageLongValues = new long[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageLongValues[cnt++] = decoder.readLong(page);
                }

                // TODO there may return many results
                for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                    while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        res.putLong(insertMemoryData.getCurrentLongValue());

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
                    }
                }
                break;
            case FLOAT:
                float[] pageFloatValues = new float[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageFloatValues[cnt++] = decoder.readFloat(page);
                }

                // TODO there may return many results
                for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                    while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        res.putFloat(insertMemoryData.getCurrentFloatValue());

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
                    }
                }
                break;
            case DOUBLE:
                double[] pageDoubleValues = new double[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageDoubleValues[cnt++] = decoder.readDouble(page);
                }

                // TODO there may return many results
                for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                    while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        res.putDouble(insertMemoryData.getCurrentDoubleValue());

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
                    }
                }
                break;
            case TEXT:
                Binary[] pageTextValues = new Binary[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageTextValues[cnt++] = decoder.readBinary(page);
                }

                // TODO there may return many results
                for (; timeIdx < pageTimestamps.length; timeIdx ++) {
                    while (insertMemoryData.hasInsertData() && timeIdx < pageTimestamps.length
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[timeIdx]) {
                        res.putTime(insertMemoryData.getCurrentMinTime());
                        res.putBinary(insertMemoryData.getCurrentBinaryValue());

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
                    }
                }
                break;
            default:
                throw new IOException("Data type not support : " + dataType);
        }
        return res;
    }

    /**
     * <p>
     * An aggregation method implementation for the DataPage aspect.
     * This method is only used for aggregation function.
     *
     * @param dataType DataPage data type
     * @param pageTimestamps the timestamps of current DataPage
     * @param decoder the decoder of DataPage
     * @param page the DataPage need to be aggregated
     * @param timeFilter time filter
     * @param commonTimestamps the timestamps which aggregation must satisfy
     * @param commonTimestampsIndex the read time index of timestamps which aggregation must satisfy
     * @param insertMemoryData bufferwrite memory insert data with overflow operation
     * @return left represents the data of DataPage which satisfies the restrict condition,
     *         right represents the read time index of commonTimestamps
     */
    public static Pair<DynamicOneColumnData, Integer> readOnePage(TSDataType dataType, long[] pageTimestamps,
                Decoder decoder, InputStream page,
                SingleSeriesFilterExpression timeFilter, List<Long> commonTimestamps, int commonTimestampsIndex,
                InsertDynamicData insertMemoryData, UpdateOperation updateOperation) throws IOException {

        //TODO optimize the logic, we could read the page data firstly, the make filter about the data, it's easy to check

        DynamicOneColumnData aggregateResult = new DynamicOneColumnData(dataType, true);
        int pageTimeIndex = 0;

        SingleValueVisitor<?> timeVisitor = null;
        if (timeFilter != null) {
            timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        }

        switch (dataType) {
            case INT32:
                int[] pageIntValues = new int[pageTimestamps.length];
                int cnt = 0;
                while (decoder.hasNext(page)) {
                    pageIntValues[cnt++] = decoder.readInt(page);
                }

                while (pageTimeIndex < pageTimestamps.length && commonTimestampsIndex < commonTimestamps.size()) {
                    long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                    while (pageTimeIndex < pageTimestamps.length && insertMemoryData.hasInsertData()
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[pageTimeIndex]) {
                        if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                            aggregateResult.putTime(insertMemoryData.getCurrentMinTime());
                            aggregateResult.putInt(insertMemoryData.getCurrentIntValue());

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[pageTimeIndex]) {
                                insertMemoryData.removeCurrentValue();
                                pageTimeIndex++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }

                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }

                        } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp) {
                            insertMemoryData.removeCurrentValue();
                        } else {
                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }
                        }
                    }

                    if (pageTimeIndex >= pageTimestamps.length || commonTimestampsIndex >= commonTimestamps.size()) {
                        break;
                    }

                    if (pageTimestamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                        if (timeFilter == null || timeVisitor.verify(pageTimestamps[pageTimeIndex])) {
                            if (updateOperation.verifyTime(pageTimestamps[pageTimeIndex])) {
                                if (updateOperation.verifyValue()) {
                                    aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                    aggregateResult.putInt(updateOperation.getInt());
                                }
                            } else {
                                aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                aggregateResult.putInt(pageIntValues[pageTimeIndex]);
                            }
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        } else {
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        }
                    } else if (pageTimestamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                        pageTimeIndex += 1;
                    } else {
                        commonTimestampsIndex += 1;
                    }
                }

                return new Pair<>(aggregateResult, commonTimestampsIndex);
            case BOOLEAN:
                boolean[] pageBooleanValues = new boolean[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageBooleanValues[cnt++] = decoder.readBoolean(page);
                }

                while (pageTimeIndex < pageTimestamps.length && commonTimestampsIndex < commonTimestamps.size()) {
                    long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                    while (pageTimeIndex < pageTimestamps.length && insertMemoryData.hasInsertData()
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[pageTimeIndex]) {
                        if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                            aggregateResult.putTime(insertMemoryData.getCurrentMinTime());
                            aggregateResult.putBoolean(insertMemoryData.getCurrentBooleanValue());

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[pageTimeIndex]) {
                                insertMemoryData.removeCurrentValue();
                                pageTimeIndex++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }

                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }

                        } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp) {
                            insertMemoryData.removeCurrentValue();
                        } else {
                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }
                        }
                    }

                    if (pageTimeIndex >= pageTimestamps.length || commonTimestampsIndex >= commonTimestamps.size()) {
                        break;
                    }

                    if (pageTimestamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                        if (timeFilter == null || timeVisitor.verify(pageTimestamps[pageTimeIndex])) {
                            if (updateOperation.verifyTime(pageTimestamps[pageTimeIndex])) {
                                if (updateOperation.verifyValue()) {
                                    aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                    aggregateResult.putBoolean(updateOperation.getBoolean());
                                }
                            } else {
                                aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                aggregateResult.putBoolean(pageBooleanValues[pageTimeIndex]);
                            }
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        } else {
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        }
                    } else if (pageTimestamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                        pageTimeIndex += 1;
                    } else {
                        commonTimestampsIndex += 1;
                    }
                }

                return new Pair<>(aggregateResult, commonTimestampsIndex);
            case INT64:
                long[] pageLongValues = new long[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageLongValues[cnt++] = decoder.readLong(page);
                }

                while (pageTimeIndex < pageTimestamps.length && commonTimestampsIndex < commonTimestamps.size()) {
                    long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                    while (pageTimeIndex < pageTimestamps.length && insertMemoryData.hasInsertData()
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[pageTimeIndex]) {
                        if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                            aggregateResult.putTime(insertMemoryData.getCurrentMinTime());
                            aggregateResult.putLong(insertMemoryData.getCurrentLongValue());

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[pageTimeIndex]) {
                                insertMemoryData.removeCurrentValue();
                                pageTimeIndex++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }

                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }

                        } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp) {
                            insertMemoryData.removeCurrentValue();
                        } else {
                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }
                        }
                    }

                    if (pageTimeIndex >= pageTimestamps.length || commonTimestampsIndex >= commonTimestamps.size()) {
                        break;
                    }

                    if (pageTimestamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                        if (timeFilter == null || timeVisitor.verify(pageTimestamps[pageTimeIndex])) {
                            if (updateOperation.verifyTime(pageTimestamps[pageTimeIndex])) {
                                if (updateOperation.verifyValue()) {
                                    aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                    aggregateResult.putLong(updateOperation.getLong());
                                }
                            } else {
                                aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                aggregateResult.putLong(pageLongValues[pageTimeIndex]);
                            }
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        } else {
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        }
                    } else if (pageTimestamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                        pageTimeIndex += 1;
                    } else {
                        commonTimestampsIndex += 1;
                    }
                }

                return new Pair<>(aggregateResult, commonTimestampsIndex);
            case FLOAT:
                float[] pageFloatValues = new float[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageFloatValues[cnt++] = decoder.readFloat(page);
                }

                while (pageTimeIndex < pageTimestamps.length && commonTimestampsIndex < commonTimestamps.size()) {
                    long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                    while (pageTimeIndex < pageTimestamps.length && insertMemoryData.hasInsertData()
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[pageTimeIndex]) {
                        if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                            aggregateResult.putTime(insertMemoryData.getCurrentMinTime());
                            aggregateResult.putFloat(insertMemoryData.getCurrentFloatValue());

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[pageTimeIndex]) {
                                insertMemoryData.removeCurrentValue();
                                pageTimeIndex++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }

                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }

                        } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp) {
                            insertMemoryData.removeCurrentValue();
                        } else {
                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }
                        }
                    }

                    if (pageTimeIndex >= pageTimestamps.length || commonTimestampsIndex >= commonTimestamps.size()) {
                        break;
                    }

                    if (pageTimestamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                        if (timeFilter == null || timeVisitor.verify(pageTimestamps[pageTimeIndex])) {
                            if (updateOperation.verifyTime(pageTimestamps[pageTimeIndex])) {
                                if (updateOperation.verifyValue()) {
                                    aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                    aggregateResult.putFloat(updateOperation.getFloat());
                                }
                            } else {
                                aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                aggregateResult.putFloat(pageFloatValues[pageTimeIndex]);
                            }
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        } else {
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        }
                    } else if (pageTimestamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                        pageTimeIndex += 1;
                    } else {
                        commonTimestampsIndex += 1;
                    }
                }

                return new Pair<>(aggregateResult, commonTimestampsIndex);
            case DOUBLE:
                double[] pageDoubleValues = new double[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageDoubleValues[cnt++] = decoder.readDouble(page);
                }

                while (pageTimeIndex < pageTimestamps.length && commonTimestampsIndex < commonTimestamps.size()) {
                    long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                    while (pageTimeIndex < pageTimestamps.length && insertMemoryData.hasInsertData()
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[pageTimeIndex]) {
                        if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                            aggregateResult.putTime(insertMemoryData.getCurrentMinTime());
                            aggregateResult.putDouble(insertMemoryData.getCurrentDoubleValue());

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[pageTimeIndex]) {
                                insertMemoryData.removeCurrentValue();
                                pageTimeIndex++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }

                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }

                        } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp) {
                            insertMemoryData.removeCurrentValue();
                        } else {
                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }
                        }
                    }

                    if (pageTimeIndex >= pageTimestamps.length || commonTimestampsIndex >= commonTimestamps.size()) {
                        break;
                    }

                    if (pageTimestamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                        if (timeFilter == null || timeVisitor.verify(pageTimestamps[pageTimeIndex])) {
                            if (updateOperation.verifyTime(pageTimestamps[pageTimeIndex])) {
                                if (updateOperation.verifyValue()) {
                                    aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                    aggregateResult.putDouble(updateOperation.getDouble());
                                }
                            } else {
                                aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                aggregateResult.putDouble(pageDoubleValues[pageTimeIndex]);
                            }
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        } else {
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        }
                    } else if (pageTimestamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                        pageTimeIndex += 1;
                    } else {
                        commonTimestampsIndex += 1;
                    }
                }

                return new Pair<>(aggregateResult, commonTimestampsIndex);
            case TEXT:
                Binary[] pageTextValues = new Binary[pageTimestamps.length];
                cnt = 0;
                while (decoder.hasNext(page)) {
                    pageTextValues[cnt++] = decoder.readBinary(page);
                }

                while (pageTimeIndex < pageTimestamps.length && commonTimestampsIndex < commonTimestamps.size()) {
                    long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                    while (pageTimeIndex < pageTimestamps.length && insertMemoryData.hasInsertData()
                            && insertMemoryData.getCurrentMinTime() <= pageTimestamps[pageTimeIndex]) {
                        if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                            aggregateResult.putTime(insertMemoryData.getCurrentMinTime());
                            aggregateResult.putBinary(insertMemoryData.getCurrentBinaryValue());

                            if (insertMemoryData.getCurrentMinTime() == pageTimestamps[pageTimeIndex]) {
                                insertMemoryData.removeCurrentValue();
                                pageTimeIndex++;
                            } else {
                                insertMemoryData.removeCurrentValue();
                            }

                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }

                        } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp) {
                            insertMemoryData.removeCurrentValue();
                        } else {
                            commonTimestampsIndex += 1;
                            if (commonTimestampsIndex < commonTimestamps.size()) {
                                commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                            } else {
                                break;
                            }
                        }
                    }

                    if (pageTimeIndex >= pageTimestamps.length || commonTimestampsIndex >= commonTimestamps.size()) {
                        break;
                    }

                    if (pageTimestamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                        if (timeFilter == null || timeVisitor.verify(pageTimestamps[pageTimeIndex])) {
                            if (updateOperation.verifyTime(pageTimestamps[pageTimeIndex])) {
                                if (updateOperation.verifyValue()) {
                                    aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                    aggregateResult.putBinary(updateOperation.getText());
                                }
                            } else {
                                aggregateResult.putTime(pageTimestamps[pageTimeIndex]);
                                aggregateResult.putBinary(pageTextValues[pageTimeIndex]);
                            }
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        } else {
                            commonTimestampsIndex += 1;
                            pageTimeIndex += 1;
                        }
                    } else if (pageTimestamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                        pageTimeIndex += 1;
                    } else {
                        commonTimestampsIndex += 1;
                    }
                }

                return new Pair<>(aggregateResult, commonTimestampsIndex);
            default:
                throw new IOException("Data type not support : " + dataType);
        }
    }
}
