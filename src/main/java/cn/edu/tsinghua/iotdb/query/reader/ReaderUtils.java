package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
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

    /**
     * -1: no updateTrue data, no updateFalse data.
     * 0: updateTrue data is first.
     * 1: updateFalse data is first.
     *
     * @param updateTrueIdx  index of updateTrue DynamicOneColumn
     * @param updateFalseIdx index of updateFalse DynamicOneColumn
     * @param updateTrue     updateTrue DynamicOneColumn
     * @param updateFalse    updateFalse DynamicOneColumn
     * @return the mode
     */
    public static int getNextMode(int updateTrueIdx, int updateFalseIdx, DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse) {
        if (updateTrueIdx > updateTrue.timeLength - 2 && updateFalseIdx > updateFalse.timeLength - 2) {
            return -1;
        } else if (updateTrueIdx <= updateTrue.timeLength - 2 && updateFalseIdx > updateFalse.timeLength - 2) {
            return 0;
        } else if (updateTrueIdx > updateTrue.timeLength - 2 && updateFalseIdx <= updateFalse.timeLength - 2) {
            return 1;
        } else {
            long t0 = updateTrue.getTime(updateTrueIdx);
            long t1 = updateFalse.getTime(updateFalseIdx);
            return t0 < t1 ? 0 : 1;
        }
    }

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
     * @param pageTimeStamps the timestamps of current DataPage
     * @param decoder the decoder of DataPage
     * @param page the DataPage need to be aggregated
     * @param timeFilter time filter
     * @param commonTimestamps the timestamps which aggregation must satisfy
     * @param commonTimestampsIndex the read time index of timestamps which aggregation must satisfy
     * @param insertMemoryData bufferwrite memory insert data with overflow operation
     * @param update an array of overflow update info, update[0] represents updateTrue,
     *               while update[1] represents updateFalse
     * @param updateIdx an array of the index of overflow update info, update[0] represents the index of
     *                  updateTrue, while update[1] represents updateFalse
     * @return left represents the data of DataPage which satisfies the restrict condition,
     *         right represents the read time index of commonTimestamps
     */
    public static Pair<DynamicOneColumnData, Integer> readOnePage(TSDataType dataType, long[] pageTimeStamps,
                Decoder decoder, InputStream page,
                SingleSeriesFilterExpression timeFilter, List<Long> commonTimestamps, int commonTimestampsIndex,
                InsertDynamicData insertMemoryData, DynamicOneColumnData[] update, int[] updateIdx) {

        //TODO optimize the logic, we could read the page data firstly, the make filter about the data, it's easy to check

        DynamicOneColumnData aggregatePathQueryResult = new DynamicOneColumnData(dataType, true);
        int pageTimeIndex = 0;

        // calculate current mode
        int mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);

        try {
            SingleValueVisitor<?> timeVisitor = null;
            if (timeFilter != null) {
                timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
            }

            switch (dataType) {
                case INT32:
                    int intValue;
                    while (decoder.hasNext(page)) {
                        long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeStamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeStamps[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                                aggregatePathQueryResult.putTime(insertMemoryData.getCurrentMinTime());
                                aggregatePathQueryResult.putInt(insertMemoryData.getCurrentIntValue());
                                aggregatePathQueryResult.insertTrueIndex++;

                                // both insertMemory value and page value should be removed
                                if (insertMemoryData.getCurrentMinTime() == pageTimeStamps[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    intValue = decoder.readInt(page);
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }

                                commonTimestampsIndex += 1;
                                if (commonTimestampsIndex < commonTimestamps.size()) {
                                    commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                                } else {
                                    break;
                                }

                            } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp){
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

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeStamps.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation

                        // no updateTrue, no updateFalse
                        if (mode == -1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex]))) {
                                    aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                    intValue = decoder.readInt(page);
                                    aggregatePathQueryResult.putInt(intValue);
                                    aggregatePathQueryResult.insertTrueIndex++;
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    intValue = decoder.readInt(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                intValue = decoder.readInt(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                intValue = decoder.readInt(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    // if updateTrue changes the original value
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putInt(update[0].getInt(updateIdx[0] / 2));
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putInt(intValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    intValue = decoder.readInt(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                intValue = decoder.readInt(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                intValue = decoder.readInt(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        logger.error("never reach here");
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putInt(intValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                }

                                commonTimestampsIndex += 1;
                                pageTimeIndex += 1;

                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                intValue = decoder.readInt(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // set the update array to next position that current time
                        while (mode != -1 && pageTimeIndex < pageTimeStamps.length
                                && pageTimeStamps[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }

                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    break;
                case BOOLEAN:
                    boolean booleanValue;
                    while (decoder.hasNext(page)) {
                        long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeStamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeStamps[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                                aggregatePathQueryResult.putTime(insertMemoryData.getCurrentMinTime());
                                aggregatePathQueryResult.putBoolean(insertMemoryData.getCurrentBooleanValue());
                                aggregatePathQueryResult.insertTrueIndex++;

                                // both insertMemory value and page value should be removed
                                if (insertMemoryData.getCurrentMinTime() == pageTimeStamps[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    booleanValue = decoder.readBoolean(page);
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }

                                commonTimestampsIndex += 1;
                                if (commonTimestampsIndex < commonTimestamps.size()) {
                                    commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                                } else {
                                    break;
                                }

                            } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp){
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

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeStamps.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation

                        // no updateTrue, no updateFalse
                        if (mode == -1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex]))) {
                                    aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                    booleanValue = decoder.readBoolean(page);
                                    aggregatePathQueryResult.putBoolean(booleanValue);
                                    aggregatePathQueryResult.insertTrueIndex++;
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    booleanValue = decoder.readBoolean(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                booleanValue = decoder.readBoolean(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                booleanValue = decoder.readBoolean(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    // if updateTrue changes the original value
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putBoolean(update[0].getBoolean(updateIdx[0] / 2));
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putBoolean(booleanValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    booleanValue = decoder.readBoolean(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                booleanValue = decoder.readBoolean(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                booleanValue = decoder.readBoolean(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        logger.error("never reach here");
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putBoolean(booleanValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                }

                                commonTimestampsIndex += 1;
                                pageTimeIndex += 1;

                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                booleanValue = decoder.readBoolean(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // set the update array to next position that current time
                        while (mode != -1 && pageTimeIndex < pageTimeStamps.length
                                && pageTimeStamps[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }

                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    break;
                case INT64:
                    long longValue;
                    while (decoder.hasNext(page)) {
                        long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeStamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeStamps[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                                aggregatePathQueryResult.putTime(insertMemoryData.getCurrentMinTime());
                                aggregatePathQueryResult.putLong(insertMemoryData.getCurrentLongValue());
                                aggregatePathQueryResult.insertTrueIndex++;

                                // both insertMemory value and page value should be removed
                                if (insertMemoryData.getCurrentMinTime() == pageTimeStamps[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    longValue = decoder.readLong(page);
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }

                                commonTimestampsIndex += 1;
                                if (commonTimestampsIndex < commonTimestamps.size()) {
                                    commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                                } else {
                                    break;
                                }

                            } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp){
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

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeStamps.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation

                        // no updateTrue, no updateFalse
                        if (mode == -1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex]))) {
                                    aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                    longValue = decoder.readLong(page);
                                    aggregatePathQueryResult.putLong(longValue);
                                    aggregatePathQueryResult.insertTrueIndex++;
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    longValue = decoder.readLong(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                longValue = decoder.readLong(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                longValue = decoder.readLong(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    // if updateTrue changes the original value
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putLong(update[0].getLong(updateIdx[0] / 2));
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putLong(longValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    longValue = decoder.readLong(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                longValue = decoder.readLong(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                longValue = decoder.readLong(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        logger.error("never reach here");
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putLong(longValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                }

                                commonTimestampsIndex += 1;
                                pageTimeIndex += 1;

                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                longValue = decoder.readLong(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // set the update array to next position that current time
                        while (mode != -1 && pageTimeIndex < pageTimeStamps.length
                                && pageTimeStamps[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }

                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    break;
                case FLOAT:
                    float floatValue;
                    while (decoder.hasNext(page)) {
                        long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeStamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeStamps[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                                aggregatePathQueryResult.putTime(insertMemoryData.getCurrentMinTime());
                                aggregatePathQueryResult.putFloat(insertMemoryData.getCurrentFloatValue());
                                aggregatePathQueryResult.insertTrueIndex++;

                                // both insertMemory value and page value should be removed
                                if (insertMemoryData.getCurrentMinTime() == pageTimeStamps[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    floatValue = decoder.readFloat(page);
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }

                                commonTimestampsIndex += 1;
                                if (commonTimestampsIndex < commonTimestamps.size()) {
                                    commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                                } else {
                                    break;
                                }

                            } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp){
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

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeStamps.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation

                        // no updateTrue, no updateFalse
                        if (mode == -1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex]))) {
                                    aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                    floatValue = decoder.readFloat(page);
                                    aggregatePathQueryResult.putFloat(floatValue);
                                    aggregatePathQueryResult.insertTrueIndex++;
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    floatValue = decoder.readFloat(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                floatValue = decoder.readFloat(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                floatValue = decoder.readFloat(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    // if updateTrue changes the original value
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putFloat(update[0].getFloat(updateIdx[0] / 2));
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putFloat(floatValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    floatValue = decoder.readFloat(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                floatValue = decoder.readFloat(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                floatValue = decoder.readFloat(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        logger.error("never reach here");
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putFloat(floatValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                }

                                commonTimestampsIndex += 1;
                                pageTimeIndex += 1;

                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                floatValue = decoder.readFloat(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // set the update array to next position that current time
                        while (mode != -1 && pageTimeIndex < pageTimeStamps.length
                                && pageTimeStamps[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }

                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    break;
                case DOUBLE:
                    double doubleValue;
                    while (decoder.hasNext(page)) {
                        long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeStamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeStamps[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                                aggregatePathQueryResult.putTime(insertMemoryData.getCurrentMinTime());
                                aggregatePathQueryResult.putDouble(insertMemoryData.getCurrentDoubleValue());
                                aggregatePathQueryResult.insertTrueIndex++;

                                // both insertMemory value and page value should be removed
                                if (insertMemoryData.getCurrentMinTime() == pageTimeStamps[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    doubleValue = decoder.readDouble(page);
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }

                                commonTimestampsIndex += 1;
                                if (commonTimestampsIndex < commonTimestamps.size()) {
                                    commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                                } else {
                                    break;
                                }

                            } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp){
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

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeStamps.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation

                        // no updateTrue, no updateFalse
                        if (mode == -1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex]))) {
                                    aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                    doubleValue = decoder.readDouble(page);
                                    aggregatePathQueryResult.putDouble(doubleValue);
                                    aggregatePathQueryResult.insertTrueIndex++;
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    doubleValue = decoder.readDouble(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                doubleValue = decoder.readDouble(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                doubleValue = decoder.readDouble(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    // if updateTrue changes the original value
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putDouble(update[0].getDouble(updateIdx[0] / 2));
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putDouble(doubleValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    doubleValue = decoder.readDouble(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                doubleValue = decoder.readDouble(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                doubleValue = decoder.readDouble(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        logger.error("never reach here");
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putDouble(doubleValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                }

                                commonTimestampsIndex += 1;
                                pageTimeIndex += 1;

                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                doubleValue = decoder.readDouble(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // set the update array to next position that current time
                        while (mode != -1 && pageTimeIndex < pageTimeStamps.length
                                && pageTimeStamps[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }

                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    break;
                case TEXT:
                    Binary binaryValue;
                    while (decoder.hasNext(page)) {
                        long commonTimestamp = commonTimestamps.get(commonTimestampsIndex);

                        while (insertMemoryData.hasInsertData() && pageTimeIndex < pageTimeStamps.length
                                && insertMemoryData.getCurrentMinTime() <= pageTimeStamps[pageTimeIndex]) {

                            if (insertMemoryData.getCurrentMinTime() == commonTimestamp) {
                                aggregatePathQueryResult.putTime(insertMemoryData.getCurrentMinTime());
                                aggregatePathQueryResult.putBinary(insertMemoryData.getCurrentBinaryValue());
                                aggregatePathQueryResult.insertTrueIndex++;

                                // both insertMemory value and page value should be removed
                                if (insertMemoryData.getCurrentMinTime() == pageTimeStamps[pageTimeIndex]) {
                                    insertMemoryData.removeCurrentValue();
                                    pageTimeIndex++;
                                    binaryValue = decoder.readBinary(page);
                                } else {
                                    insertMemoryData.removeCurrentValue();
                                }

                                commonTimestampsIndex += 1;
                                if (commonTimestampsIndex < commonTimestamps.size()) {
                                    commonTimestamp = commonTimestamps.get(commonTimestampsIndex);
                                } else {
                                    break;
                                }

                            } else if (insertMemoryData.getCurrentMinTime() < commonTimestamp){
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

                        if (!decoder.hasNext(page) || pageTimeIndex >= pageTimeStamps.length) {
                            break;
                        }
                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }

                        // compare with commonTimestamps firstly
                        // then compare with time filter
                        // lastly compare with update operation

                        // no updateTrue, no updateFalse
                        if (mode == -1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                if ((timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex]))) {
                                    aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                    binaryValue = decoder.readBinary(page);
                                    aggregatePathQueryResult.putBinary(binaryValue);
                                    aggregatePathQueryResult.insertTrueIndex++;
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    binaryValue = decoder.readBinary(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                binaryValue = decoder.readBinary(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 0) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                binaryValue = decoder.readBinary(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    // if updateTrue changes the original value
                                    if (update[0].getTime(updateIdx[0]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[0].getTime(updateIdx[0] + 1)) {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putBinary(update[0].getBinary(updateIdx[0] / 2));
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putBinary(binaryValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                } else {
                                    binaryValue = decoder.readBinary(page);
                                    commonTimestampsIndex += 1;
                                    pageTimeIndex += 1;
                                }
                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                binaryValue = decoder.readBinary(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        } else if (mode == 1) {
                            if (pageTimeStamps[pageTimeIndex] == commonTimestamps.get(commonTimestampsIndex)) {
                                binaryValue = decoder.readBinary(page);

                                if (timeFilter == null || timeVisitor.verify(pageTimeStamps[pageTimeIndex])) {
                                    if (update[1].getTime(updateIdx[1]) <= pageTimeStamps[pageTimeIndex]
                                            && pageTimeStamps[pageTimeIndex] <= update[1].getTime(updateIdx[1] + 1)) {
                                        logger.error("never reach here");
                                    } else {
                                        aggregatePathQueryResult.putTime(pageTimeStamps[pageTimeIndex]);
                                        aggregatePathQueryResult.putBinary(binaryValue);
                                        aggregatePathQueryResult.insertTrueIndex++;
                                    }
                                }

                                commonTimestampsIndex += 1;
                                pageTimeIndex += 1;

                            } else if (pageTimeStamps[pageTimeIndex] < commonTimestamps.get(commonTimestampsIndex)) {
                                binaryValue = decoder.readBinary(page);
                                pageTimeIndex += 1;
                            } else {
                                commonTimestampsIndex += 1;
                            }
                        }

                        // set the update array to next position that current time
                        while (mode != -1 && pageTimeIndex < pageTimeStamps.length
                                && pageTimeStamps[pageTimeIndex] > update[mode].getTime(updateIdx[mode] + 1)) {
                            updateIdx[mode] += 2;
                            mode = getNextMode(updateIdx[0], updateIdx[1], update[0], update[1]);
                        }

                        if (commonTimestampsIndex >= commonTimestamps.size()) {
                            break;
                        }
                    }

                    // still has page data, no common timestamps
                    if (decoder.hasNext(page) && commonTimestampsIndex >= commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    // still has common timestamps, no page data
                    if (!decoder.hasNext(page) && commonTimestampsIndex < commonTimestamps.size()) {
                        return new Pair<>(aggregatePathQueryResult, commonTimestampsIndex);
                    }

                    break;
                default:
                    throw new IOException("Data type not support : " + dataType);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // update the curIdx in updateTrue and updateFalse
        update[0].curIdx = updateIdx[0];
        update[1].curIdx = updateIdx[1];
        return new Pair<>(aggregatePathQueryResult, 0);
    }
}
