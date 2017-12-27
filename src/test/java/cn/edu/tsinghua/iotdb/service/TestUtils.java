package cn.edu.tsinghua.iotdb.service;


import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class TestUtils {

    static boolean testFlag = true;

    public static String insertTemplate = "insert into %s(timestamp%s) values(%d%s)";

    static String count(String path) {
        return String.format("count(%s)", path);
    }

    static String max_time(String path) {
        return String.format("max_time(%s)", path);
    }

    static String min_time(String path) {
        return String.format("min_time(%s)", path);
    }

    static String max_value(String path) {
        return String.format("max_value(%s)", path);
    }

    static String min_value(String path) {
        return String.format("min_value(%s)", path);
    }

    public static String recordToInsert(TSRecord record) {
        StringBuilder measurements = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for(DataPoint dataPoint : record.dataPointList) {
            measurements.append(",").append(dataPoint.getMeasurementId());
            values.append(",").append(dataPoint.getValue());
        }
        return String.format(insertTemplate, record.deltaObjectId, measurements.toString(),record.time, values);
    }
}
