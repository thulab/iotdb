package cn.edu.tsinghua.iotdb.MonitorV2.Event;

import cn.edu.tsinghua.iotdb.MonitorV2.MonitorConstants;
import cn.edu.tsinghua.iotdb.MonitorV2.StatMonitor;
import cn.edu.tsinghua.iotdb.MonitorV2.StatisticTSRecord;

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

        String stat_path = MonitorConstants.convertStorageGroupPathToStatisticPath(path);
        StatisticTSRecord record = new StatisticTSRecord(timestamp, stat_path);
        record.addOneStatistic(StatisticTSRecord.StatisticConstants.TOTAL_POINTS_SUCCESS, value);
        return record;
    }
}
