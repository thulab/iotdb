import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    static int fetchSize = 100000;
    static int deviceStart = 0, deviceEnd = 10;
    static int sensorStart = 0, sensorEnd = 10;

    public static void main(String[] args) throws ClassNotFoundException, PathErrorException, IOException, ProcessorException {

        queryWithoutFilterTest();

    }

    public static void queryWithoutFilterTest() throws PathErrorException, IOException, ProcessorException {

        List<Path> pathList = new ArrayList<>();

        for (int i = deviceStart; i <= deviceEnd; i++) {
            for (int j = sensorStart; j <= sensorEnd; j++) {
                pathList.add(getPath(i, j));
            }
        }

        long startTime = System.currentTimeMillis();

        OverflowQueryEngine queryEngine = new OverflowQueryEngine();

        QueryDataSet dataSet =
                queryEngine.query(0, pathList, null, null, null, null, fetchSize, null);

        int cnt = 0;
        while (dataSet.hasNextRecord()) {
            RowRecord rowRecord = dataSet.getNextRecord();
            cnt ++;
            //System.out.println(rowRecord);
        }

        System.out.println(cnt);

        long endTime = System.currentTimeMillis();
        System.out.println("Time consume : " + (endTime - startTime));
    }

    public static Path getPath(int d, int s) {
        return new Path(String.format("root.performf.group_0.d_%s.s_%s", d, s));
    }
}
