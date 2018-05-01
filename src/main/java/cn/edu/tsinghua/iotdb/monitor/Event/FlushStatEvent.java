package cn.edu.tsinghua.iotdb.monitor.Event;

public class FlushStatEvent extends StatEvent {

    public FlushStatEvent(long timestamp, String path, long value) {
        super(timestamp, path, value);
    }
}
