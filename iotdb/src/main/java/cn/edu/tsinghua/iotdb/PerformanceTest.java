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

    private static int deviceStart = 1, deviceEnd = 1;
    private static int sensorStart = 1, sensorEnd = 1;

    public static void main(String[] args) throws IOException, FileNodeManagerException {
        //IoTDB.getInstance().active();
        queryWithoutFilterTest();
    }

    public static void queryWithoutFilterTest() throws IOException, FileNodeManagerException {

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
            if (count % 10000 == 0) {
                System.out.println("====" + count + "  " + rowRecord);
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));
    }

    public static Path getPath(int d, int s) {
        return new Path(String.format("root.perform.group_0.d_%s.s_%s", d, s));
    }
}
