package cn.edu.tsinghua.iotdb.engine.flushthread;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FlushManager {

    private ExecutorService pool;

    private static class InstanceHolder {
        private static FlushManager instance = new FlushManager();
    }

    private FlushManager() {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        pool = Executors.newFixedThreadPool(config.concurrentFlushThread);
    }

    public FlushManager getInstance(){
        return InstanceHolder.instance;
    }

    synchronized public void submit(Runnable task) {
        pool.submit(task);
    }

}
