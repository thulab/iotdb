package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.monitor.Event.StatEvent;

public interface StatEventProducer {

    public void registerListener(StatEventListener listener);

    public void produceEvent(StatEvent event);
}
