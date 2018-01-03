package cn.edu.tsinghua.iotdb.query.engine;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.and;

/**
 * Take out some common methods used for QueryEngine.
 */
public class EngineUtils {

    /**
     * QueryDataSet.BatchReadGenerator has calculated and removed the common RowRecord timestamps in the top of heap.
     * For the reason of that the RowRecord number is greater than fetch size,
     * so there may be remaining data in QueryDataSet.BatchReadGenerator,
     */
    public static void putRecordFromBatchReadGenerator(QueryDataSet dataSet) {
        for (Path path : dataSet.getBatchReadGenerator().retMap.keySet()) {
            DynamicOneColumnData batchReadData = dataSet.getBatchReadGenerator().retMap.get(path);
            DynamicOneColumnData leftData = batchReadData.sub(batchReadData.curIdx);

            // copy batch read info from oneColRet to leftRet
            batchReadData.copyFetchInfoTo(leftData);
            dataSet.getBatchReadGenerator().retMap.put(path, leftData);
            batchReadData.rollBack(batchReadData.valueLength - batchReadData.curIdx);
            dataSet.mapRet.put(path.getFullPath(), batchReadData);
        }
    }

    /**
     *  Merge the overflow insert data with the bufferwrite insert data.
     */
    public static List<Object> getOverflowMergedWithLastPageData(SingleSeriesFilterExpression queryTimeFilter,
                                                                 SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter,
                                                                 DynamicOneColumnData res, DynamicOneColumnData lastPageData, List<Object> overflowParams) {

        List<Object> paramList = new ArrayList<>();

        // time filter of overflow is not null,
        // new time filter should be an intersection of query time filter and overflow time filter
        if (overflowParams.get(3) != null) {
            if (queryTimeFilter != null)
                queryTimeFilter = (SingleSeriesFilterExpression) and(queryTimeFilter, (SingleSeriesFilterExpression) overflowParams.get(3));
            else
                queryTimeFilter = (SingleSeriesFilterExpression) overflowParams.get(3);
        }

        DynamicOneColumnData updateTrue = (DynamicOneColumnData) overflowParams.get(1);
        lastPageData = getSatisfiedData(updateTrue, queryTimeFilter, valueFilter, lastPageData);

        DynamicOneColumnData overflowInsertTrue = (DynamicOneColumnData) overflowParams.get(0);
        if (overflowInsertTrue == null) {
            overflowInsertTrue = lastPageData;
        } else {
            overflowInsertTrue = mergeOverflowAndLastPage(overflowInsertTrue, lastPageData);
        }
        paramList.add(overflowInsertTrue);
        paramList.add(overflowParams.get(1));
        paramList.add(overflowParams.get(2));
        paramList.add(queryTimeFilter);

        return paramList;
    }

    /**
     * Get satisfied values from a DynamicOneColumnData.
     *
     */
    private static DynamicOneColumnData getSatisfiedData(DynamicOneColumnData updateTrue,
                                                         SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                                         DynamicOneColumnData lastPageData) {
        if (lastPageData == null) {
            return null;
        }
        if (lastPageData.valueLength == 0) {
            return lastPageData;
        }

        lastPageData = updateValueAccordingToUpdateTrue(updateTrue, lastPageData);
        DynamicOneColumnData res = new DynamicOneColumnData(lastPageData.dataType, true);
        SingleValueVisitor<?> timeVisitor = null;
        if (timeFilter != null) {
            timeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, timeFilter);
        }
        SingleValueVisitor<?> valueVisitor = null;
        if (valueFilter != null) {
            valueVisitor = getSingleValueVisitorByDataType(lastPageData.dataType, valueFilter);
        }

        switch (lastPageData.dataType) {
            case BOOLEAN:
                for (int i = 0; i < lastPageData.valueLength; i++) {
                    boolean v = lastPageData.getBoolean(i);
                    if ((timeFilter == null || timeVisitor.verify(lastPageData.getTime(i))) &&
                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                        res.putBoolean(v);
                        res.putTime(lastPageData.getTime(i));
                    }
                }
                break;
            case INT32:
                for (int i = 0; i < lastPageData.valueLength; i++) {
                    int v = lastPageData.getInt(i);
                    if ((timeFilter == null || timeVisitor.verify(lastPageData.getTime(i))) &&
                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                        res.putInt(v);
                        res.putTime(lastPageData.getTime(i));
                    }
                }
                break;
            case INT64:
                for (int i = 0; i < lastPageData.valueLength; i++) {
                    long v = lastPageData.getLong(i);
                    if ((timeFilter == null || timeVisitor.verify(lastPageData.getTime(i))) &&
                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                        res.putLong(v);
                        res.putTime(lastPageData.getTime(i));
                    }
                }
                break;
            case FLOAT:
                for (int i = 0; i < lastPageData.valueLength; i++) {
                    float v = lastPageData.getFloat(i);
                    if ((timeFilter == null || timeVisitor.verify(lastPageData.getTime(i))) &&
                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                        res.putFloat(v);
                        res.putTime(lastPageData.getTime(i));
                    }
                }
                break;
            case DOUBLE:
                for (int i = 0; i < lastPageData.valueLength; i++) {
                    double v = lastPageData.getDouble(i);
                    if ((timeFilter == null || timeVisitor.verify(lastPageData.getTime(i))) &&
                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                        res.putDouble(v);
                        res.putTime(lastPageData.getTime(i));
                    }
                }
                break;
            case TEXT:
                for (int i = 0; i < lastPageData.valueLength; i++) {
                    Binary v = lastPageData.getBinary(i);
                    if ((timeFilter == null || timeVisitor.verify(lastPageData.getTime(i))) &&
                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                        res.putBinary(v);
                        res.putTime(lastPageData.getTime(i));
                    }
                }
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported data type for read: " + lastPageData.dataType);
        }

        return res;
    }

    private static DynamicOneColumnData updateValueAccordingToUpdateTrue(DynamicOneColumnData updateTrue
            , DynamicOneColumnData lastPageData) {

        if (lastPageData == null) {
            return null;
        }

        int idx = 0;
        for (int i = 0; i < updateTrue.valueLength; i++) {
            while (idx < lastPageData.valueLength && updateTrue.getTime(i * 2 + 1) >= lastPageData.getTime(idx)) {
                if (updateTrue.getTime(i * 2) <= lastPageData.getTime(idx)) {
                    switch (lastPageData.dataType) {
                        case BOOLEAN:
                            lastPageData.setBoolean(idx, updateTrue.getBoolean(i));
                            break;
                        case INT32:
                            lastPageData.setInt(idx, updateTrue.getInt(i));
                            break;
                        case INT64:
                            lastPageData.setLong(idx, updateTrue.getLong(i));
                            break;
                        case FLOAT:
                            lastPageData.setFloat(idx, updateTrue.getFloat(i));
                            break;
                        case DOUBLE:
                            lastPageData.setDouble(idx, updateTrue.getDouble(i));
                            break;
                        case TEXT:
                            lastPageData.setBinary(idx, updateTrue.getBinary(i));
                            break;
                        case ENUMS:
                        default:
                            throw new UnSupportedDataTypeException(String.valueOf(lastPageData.dataType));
                    }
                }
                idx++;
            }
            if (idx >= lastPageData.valueLength) {
                break;
            }
        }

        return lastPageData;
    }

    private static SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
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
     * Merge insert data in overflow and buffer writer memory.<br>
     * Important: If there is two fields whose timestamp are equal, use the value
     * from overflow.
     *
     * @param overflowInsertData data in overflow insert
     * @param lastPageData data in buffer write insert
     * @return merge result of the overflow and memory insert
     */
    private static DynamicOneColumnData mergeOverflowAndLastPage(DynamicOneColumnData overflowInsertData, DynamicOneColumnData lastPageData) {
        if (overflowInsertData == null && lastPageData == null) {
            return null;
        } else if (overflowInsertData != null && lastPageData == null) {
            return overflowInsertData;
        } else if (overflowInsertData == null) {
            return lastPageData;
        }

        DynamicOneColumnData res = new DynamicOneColumnData(overflowInsertData.dataType, true);
        int overflowInsertIdx = 0;
        int lastPageIdx = 0;
        while (overflowInsertIdx < overflowInsertData.valueLength || lastPageIdx < lastPageData.valueLength) {
            while (overflowInsertIdx < overflowInsertData.valueLength && (lastPageIdx >= lastPageData.valueLength ||
                    lastPageData.getTime(lastPageIdx) >= overflowInsertData.getTime(overflowInsertIdx))) {
                res.putTime(overflowInsertData.getTime(overflowInsertIdx));
                res.putAValueFromDynamicOneColumnData(overflowInsertData, overflowInsertIdx);
                if (lastPageIdx < lastPageData.valueLength && lastPageData.getTime(lastPageIdx) == overflowInsertData.getTime(overflowInsertIdx)) {
                    lastPageIdx++;
                }
                overflowInsertIdx++;
            }

            while (lastPageIdx < lastPageData.valueLength && (overflowInsertIdx >= overflowInsertData.valueLength ||
                    overflowInsertData.getTime(overflowInsertIdx) > lastPageData.getTime(lastPageIdx))) {
                res.putTime(lastPageData.getTime(lastPageIdx));
                res.putAValueFromDynamicOneColumnData(lastPageData, lastPageIdx);
                lastPageIdx++;
            }
        }

        return res;
    }

    public static String aggregationKey(AggregateFunction aggregateFunction, Path path) {
        return aggregateFunction.name + "(" + path.getFullPath() + ")";
    }

    public static boolean noFilterOrOnlyHasTimeFilter(List<FilterStructure> filterStructures) {
        if (filterStructures == null || filterStructures.size() == 0
                || (filterStructures.size() == 1 && filterStructures.get(0).noFilter())
                || (filterStructures.size() == 1 && filterStructures.get(0).onlyHasTimeFilter())) {
            return true;
        }
        return false;
    }

    public static DynamicOneColumnData copy(DynamicOneColumnData data) {
        if (data == null) {
            // if data is null, return a DynamicOneColumnData which has no value
            return new DynamicOneColumnData(TSDataType.INT64, true);
        }

        DynamicOneColumnData ans = new DynamicOneColumnData(data.dataType, true);
        for (int i = 0;i < data.timeLength;i ++) {
            ans.putTime(data.getTime(i));
        }
        switch (data.dataType) {
            case INT32:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putInt(data.getInt(i));
                }
                break;
            case INT64:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putLong(data.getLong(i));
                }
                break;
            case FLOAT:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putFloat(data.getFloat(i));
                }
                break;
            case DOUBLE:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putDouble(data.getDouble(i));
                }
                break;
            case BOOLEAN:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putBoolean(data.getBoolean(i));
                }
                break;
            case TEXT:
                for (int i = 0;i < data.valueLength;i++) {
                    ans.putBinary(data.getBinary(i));
                }
                break;
            default:
                break;
        }

        return ans;
    }
}