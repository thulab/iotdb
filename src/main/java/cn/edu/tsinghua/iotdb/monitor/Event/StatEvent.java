package cn.edu.tsinghua.iotdb.monitor.Event;

import cn.edu.tsinghua.iotdb.monitor.MonitorConstants;
import cn.edu.tsinghua.iotdb.monitor.StatTSRecord;

public class StatEvent {

    private long timestamp;
    private String path;
    private long value; //positive value means adding value, negative value means deleting value

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

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public StatTSRecord convertToStatTSRecord(){
        String stat_path = MonitorConstants.convertStorageGroupPathToStatisticPath(path);
        StatTSRecord record = new StatTSRecord(timestamp, stat_path);
        record.addOneStatistic(MonitorConstants.StatisticConstants.TOTAL_POINTS_SUCCESS, value);
        return record;
    }
}
