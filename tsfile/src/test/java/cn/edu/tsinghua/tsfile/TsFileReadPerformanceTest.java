package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecordV2;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.DataSetWithoutFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.DynamicOneColumnData;

import java.io.IOException;

public class TsFileReadPerformanceTest {
    static String filePath = "/Users/beyyes/Desktop/test.tsfile";

    static int deviceNum = 1;
    static int sensorNum = 1;
    static int SIZE = 10000000;

    public static void main(String args[]) throws IOException, InterruptedException {

        //TimeUnit.SECONDS.sleep(10);

        readTestV8WithoutFilter();

    }

    private static void dynamicWithTsPrimitiveTest() {
        DynamicOneColumnData dynamicData = new DynamicOneColumnData(TSDataType.INT32, true);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            dynamicData.putTime(i);
            dynamicData.putInt(i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("before, consume time : %sms", endTime - startTime));

        startTime = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            TimeValuePair tp = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
        }
        endTime = System.currentTimeMillis();
        System.out.println(String.format("after, consume time : %sms", endTime - startTime));
    }

    private static void readTestV8WithoutFilter() throws IOException {
        TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        ReadOnlyTsFile tsFile = new ReadOnlyTsFile(reader);
        QueryExpression queryExpression = QueryExpression.create();
        for (int i = 1; i <= deviceNum; i++) {
            for (int j = 0; j < sensorNum; j++) {
                queryExpression.addSelectedPath(new Path(getPerformPath(i, j)));
            }
        }

        long startTime = System.currentTimeMillis();
        int cnt = 0;
        DataSetWithoutFilter queryDataSet = (DataSetWithoutFilter) tsFile.query(queryExpression);
        while (queryDataSet.hasNextRowRecord()) {
            RowRecordV2 record = queryDataSet.nextRowRecord();
//            System.out.println(record.toString());
//            if (cnt % 5000 == 0) {
//                System.out.println(record.toString());
//            }
            cnt++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("consume time : %sms, row count : %s", endTime - startTime, cnt));
        tsFile.close();
    }

    private static String getPerformPath(int device, int sensor) {
        return String.format("d%s.s%s", device, sensor);
    }
}
