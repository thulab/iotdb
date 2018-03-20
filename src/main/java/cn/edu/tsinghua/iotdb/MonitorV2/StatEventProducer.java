package cn.edu.tsinghua.iotdb.MonitorV2;

import cn.edu.tsinghua.iotdb.MonitorV2.Event.StatEvent;

public interface StatEventProducer {

    public void registerListener(StatEventListener listener);

    public void produceEvent(StatEvent event);
}
