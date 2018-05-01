package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.monitor.Event.StatEvent;

public interface StatEventListener {

    public abstract void addEvent(StatEvent event);

    public abstract void dealWithEvent(StatEvent event);
}
