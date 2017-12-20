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

    static public FlushManager getInstance(){
        return InstanceHolder.instance;
    }

    /**
     * Block new flush submits and exit when all RUNNING THREAD in the pool end.
     */
    public void forceClose() {
        pool.shutdownNow();
    }

    /**
     * Block new flush submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
     */
    public void close() {
        pool.shutdown();
    }

    synchronized public void submit(Runnable task) {
        pool.submit(task);
    }

}
