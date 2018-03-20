package cn.edu.tsinghua.iotdb.MonitorV2.Event;

import cn.edu.tsinghua.iotdb.MonitorV2.EventConstants;
import cn.edu.tsinghua.iotdb.MonitorV2.MonitorConstants;

public class CoverStatEvent extends StatEvent {

    public CoverStatEvent(long timestamp, String path, long before, long after) {
        super(timestamp, path, after - before);
    }
}
