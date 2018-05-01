package cn.edu.tsinghua.iotdb.monitor.Event;

public class CoverStatEvent extends StatEvent {

    public CoverStatEvent(long timestamp, String path, long before, long after) {
        super(timestamp, path, after - before);
    }
}
