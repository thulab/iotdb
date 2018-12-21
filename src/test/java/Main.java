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

    static int fetchSize = 10000;

    public static void main(String[] args) throws ClassNotFoundException, PathErrorException, IOException, ProcessorException {

        OverflowQueryEngine queryEngine = new OverflowQueryEngine();

        List<Path> pathList = new ArrayList<>();
        pathList.add(new Path(getPath(0, 0)));

        QueryDataSet dataSet =
                queryEngine.query(0, pathList, null, null, null, null, fetchSize, null);

        int cnt = 0;
        while (dataSet.hasNextRecord()) {
            RowRecord rowRecord = dataSet.getNextRecord();
            System.out.println(rowRecord);
        }

    }

    public static String getPath(int d, int s) {
        return String.format("root.performf.group_0.d_%s.s_%s", d, s);
    }
}
