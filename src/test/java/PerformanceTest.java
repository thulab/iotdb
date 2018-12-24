import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PerformanceTest {

    static int fetchSize = 100000;
    static int deviceStart = 9, deviceEnd = 9;
    static int sensorStart = 10, sensorEnd = 10;

    public static void main(String[] args) throws PathErrorException, IOException, ProcessorException {

        //queryWithoutFilterTest();

        singleQueryWithoutFilterTest();
    }

    public static void queryWithoutFilterTest() throws PathErrorException, IOException, ProcessorException {

        for (int start1 = 10; start1 >= 0; start1--) {
            for (int start2 = 10; start2 >= 10; start2--) {

                List<Path> pathList = new ArrayList<>();

                for (int i = start1; i <= deviceEnd; i++) {
                    for (int j = start2; j <= sensorEnd; j++) {
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
//                if (cnt % 10000 == 0) {
//                    System.out.println(rowRecord);
//                }
                    } else {
                        dataSet = queryEngine.query(0, pathList, null, null, null, dataSet, fetchSize, null);
                        if (!dataSet.hasNextRecord()) {
                            break;
                        }
                    }
                }

                long endTime = System.currentTimeMillis();
                System.out.println(String.format("start1:%s, start2:%s. Time consume : %s, count number : %s",
                        start1, start2, endTime - startTime, cnt));

            }
        }
    }

    public static void singleQueryWithoutFilterTest() throws PathErrorException, IOException, ProcessorException {

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
                output(cnt, rowRecord, true);
            } else {
                dataSet = queryEngine.query(0, pathList, null, null, null, dataSet, fetchSize, null);
                if (!dataSet.hasNextRecord()) {
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("start1:%s, start2:%s. Time consume : %s, count number : %s",
                deviceStart, deviceEnd, endTime - startTime, cnt));

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
