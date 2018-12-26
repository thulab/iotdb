import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.DoubleFilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTest {

    static int fetchSize = 100000;
    static int deviceStart = 9, deviceEnd = 9;
    static int sensorStart = 9, sensorEnd = 9;

    public static void main(String[] args) throws PathErrorException, IOException, ProcessorException {

        //singleSeriesQueryWithoutFilterTest();

        //queryMultiSeriesWithoutFilterTest();

        queryMultiSeriesWithFilterTest();

    }

    private static void singleSeriesQueryWithoutFilterTest() throws PathErrorException, IOException, ProcessorException {

        List<Path> pathList = new ArrayList<>();

        pathList.add(getPath(1, 1));

        OverflowQueryEngine queryEngine = new OverflowQueryEngine();

        long startTime = System.currentTimeMillis();

        QueryDataSet dataSet =
                queryEngine.query(0, pathList, null, null, null, null, fetchSize, null);

        int cnt = 0;
        while (true) {
            if (dataSet.hasNextRecord()) {
                RowRecord rowRecord = dataSet.getNextRecord();
                cnt++;
                //output(cnt, rowRecord, true);
            } else {
                dataSet = queryEngine.query(0, pathList, null, null, null, dataSet, fetchSize, null);
                if (!dataSet.hasNextRecord()) {
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("consume time : %s, count number : %s", endTime - startTime, cnt));

    }

    private static void queryMultiSeriesWithoutFilterTest() throws PathErrorException, IOException, ProcessorException {

        List<Path> pathList = new ArrayList<>();

        for (int i = deviceStart; i <= deviceEnd; i++) {
            for (int j = sensorStart; j <= sensorEnd; j++) {
                pathList.add(getPath(i, j));
            }
        }

        OverflowQueryEngine queryEngine = new OverflowQueryEngine();

        long startTime = System.currentTimeMillis();

        QueryDataSet dataSet =
                queryEngine.query(0, pathList, null, null, null, null, fetchSize, null);

        int cnt = 0;
        while (true) {
            if (dataSet.hasNextRecord()) {
                RowRecord rowRecord = dataSet.getNextRecord();
                cnt++;
            } else {
                dataSet = queryEngine.query(0, pathList, null, null, null, dataSet, fetchSize, null);
                if (!dataSet.hasNextRecord()) {
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time consume : %s, count number : %s", endTime - startTime, cnt));

    }

    private static void queryMultiSeriesWithFilterTest() throws PathErrorException, IOException, ProcessorException {


        List<Path> pathList = new ArrayList<>();

        for (int i = deviceStart; i <= deviceEnd; i++) {
            for (int j = sensorStart; j <= sensorEnd; j++) {
                pathList.add(getPath(i, j));
            }
        }

        OverflowQueryEngine queryEngine = new OverflowQueryEngine();

        long startTime = System.currentTimeMillis();

        FilterSeries<Double> filterSeries = new DoubleFilterSeries("root.perform.group_0.d_9",
                "s_9", TSDataType.DOUBLE, FilterSeriesType.VALUE_FILTER);

        FilterExpression valueExpression = FilterFactory.gtEq(filterSeries, 34300.0, true);

        FilterExpression timeExpression = FilterFactory.and(FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 1536396840000L, true),
                FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 1537736665000L, true));

        valueExpression = null;

        QueryDataSet dataSet =
                queryEngine.query(0, pathList, timeExpression, timeExpression, valueExpression, null, fetchSize, null);

        int count = 0;
        while (true) {
            if (dataSet.hasNextRecord()) {
                RowRecord rowRecord = dataSet.getNextRecord();
                count++;
//                if (count % 10000 == 0)
//                    System.out.println(rowRecord);
            } else {
                dataSet = queryEngine.query(0, pathList, timeExpression, null, valueExpression, dataSet, fetchSize, null);
                if (!dataSet.hasNextRecord()) {
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));


    }

    public static void output(int cnt, RowRecord rowRecord, boolean flag) {
        if (!flag)
            return;

        if (cnt % 10000 == 0) {
            System.out.println(cnt + " : " + rowRecord);
        }

        if (cnt > 97600) {
            System.out.println("----" + cnt + " : " + rowRecord);
        }
    }

    public static Path getPath(int d, int s) {
        return new Path(String.format("root.perform.group_0.d_%s.s_%s", d, s));
    }
}
