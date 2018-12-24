package cn.edu.tsinghua.iotdb;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.executor.EngineQueryRouter;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Delete this class when submitting pr.
 */
public class PerformanceTest {

    private static int deviceStart = 9, deviceEnd = 9;
    private static int sensorStart = 10, sensorEnd = 10;

    public static void main(String[] args) throws IOException, FileNodeManagerException {
        //queryWithoutFilterTest();
        singleWithoutFilterTest();
    }

    public static void queryWithoutFilterTest() throws IOException, FileNodeManagerException {

        for (int start1 = 10; start1 >= 0; start1--) {
            for (int start2 = 10; start2 >= 10; start2--) {

                List<Path> selectedPathList = new ArrayList<>();
                for (int i = start1; i <= deviceEnd; i++) {
                    for (int j = start2; j <= sensorEnd; j++) {
                        selectedPathList.add(getPath(i, j));
                    }
                }

                QueryExpression queryExpression = QueryExpression.create(selectedPathList, null);

                EngineQueryRouter queryRouter = new EngineQueryRouter();

                long startTime = System.currentTimeMillis();

                QueryDataSet queryDataSet = queryRouter.query(queryExpression);

                int count = 0;
                while (queryDataSet.hasNext()) {
                    RowRecord rowRecord = queryDataSet.next();
                    count++;
//            if (count % 10000 == 0) {
//                System.out.println(rowRecord);
//            }
                }

                long endTime = System.currentTimeMillis();
                System.out.println(String.format("start1:%s, start2:%s. Time consume : %s, count number : %s", start1, start2, endTime - startTime, count));
            }
        }
    }

    public static void singleWithoutFilterTest() throws IOException, FileNodeManagerException {

        List<Path> selectedPathList = new ArrayList<>();
        for (int i = deviceStart; i <= deviceEnd; i++) {
            for (int j = sensorStart; j <= sensorEnd; j++) {
                selectedPathList.add(getPath(i, j));
            }
        }

        QueryExpression queryExpression = QueryExpression.create(selectedPathList, null);

        EngineQueryRouter queryRouter = new EngineQueryRouter();

        long startTime = System.currentTimeMillis();

        QueryDataSet queryDataSet = queryRouter.query(queryExpression);

        int count = 0;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            count++;
            output(count, rowRecord, true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("start:%s, end:%s. Time consume : %s, count number : %s", deviceStart, deviceEnd, endTime - startTime, count));

    }

    public static void output(int cnt, RowRecord rowRecord, boolean flag) {
        if (!flag) {
            return;
        }

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
