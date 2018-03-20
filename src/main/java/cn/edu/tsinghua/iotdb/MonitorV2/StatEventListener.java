package cn.edu.tsinghua.iotdb.MonitorV2;

import cn.edu.tsinghua.iotdb.MonitorV2.Event.StatEvent;

public interface StatEventListener {

    public abstract void addEvent(StatEvent event);

    public abstract void dealWithEvent(StatEvent event);
}
