package cn.edu.tsinghua.iotdb.MonitorV2.Event;

import cn.edu.tsinghua.iotdb.MonitorV2.EventConstants;
import cn.edu.tsinghua.iotdb.MonitorV2.MonitorConstants;
import cn.edu.tsinghua.iotdb.MonitorV2.StatisticTSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StatEvent {

    private long timestamp;
    private String path;
    private MonitorConstants.FileNodeManagerStatConstants type;
    private long value;

    public StatEvent(long timestamp, String path, long value) {
        this.timestamp = timestamp;
        this.path = path;
        this.value = value;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public MonitorConstants.FileNodeManagerStatConstants getType() {
        return type;
    }

    public void setType(MonitorConstants.FileNodeManagerStatConstants type) {
        this.type = type;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public StatisticTSRecord convertToStatTSRecord(){
        String suffix = getPath().substring(getPath().indexOf(MonitorConstants.StorageGroupPrefix) + MonitorConstants.StorageGroupPrefix.length() + 1);
        String path = MonitorConstants.statStorageGroupPrefix + MonitorConstants.MONITOR_PATH_SEPERATOR + suffix;
        StatisticTSRecord record = new StatisticTSRecord(timestamp, path);
        record.addOneStatistic(StatisticTSRecord.StatisticConstants.TOTAL_POINTS_SUCCESS, value);
//        tsRecord.dataPointList = new ArrayList<DataPoint>() {{
//            for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
//                AtomicLong value = (AtomicLong) entry.getValue();
//                add(new LongDataPoint(entry.getKey(), value.get()));
//            }
//        }};
        return record;
    }
}
