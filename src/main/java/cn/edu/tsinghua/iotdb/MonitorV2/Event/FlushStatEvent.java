package cn.edu.tsinghua.iotdb.MonitorV2.Event;

import cn.edu.tsinghua.iotdb.MonitorV2.EventConstants;
import cn.edu.tsinghua.iotdb.MonitorV2.MonitorConstants;

public class FlushStatEvent extends StatEvent {

    public FlushStatEvent(long timestamp, String path, long value) {
        super(timestamp, path, value);
    }
}
