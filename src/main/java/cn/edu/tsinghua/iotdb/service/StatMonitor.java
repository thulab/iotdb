package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogNode;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author liliang
 */
public class StatMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatMonitor.class);
    private HashMap<String, StatProcessor> registProcessor;

    // Design for mutual access service
    private final ReadWriteLock lock;
    private ScheduledExecutorService service;

    /**
     * stats params
     */

    class statBackLoop implements Runnable {
        public void run() {
            try {
                HashMap<String, TSRecord> tsRecordHashMap = gatherStatistics();
                insert(tsRecordHashMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void registerStatistics(String path, StatProcessor statprocessor){
        registProcessor.put(path, statprocessor);
    }

    public synchronized void deregisterStatistics(String path){
        registProcessor.remove(path);
    }

    public StatMonitor(){
        try {
            MManager.getInstance().setStorageLevelToMTree("root.statistics");
        } catch (PathErrorException | IOException e) {
            LOGGER.error("MManager.getInstance().setStorageLevelToMTree False");
        }
        lock = new ReentrantReadWriteLock();
        registProcessor = new HashMap<>();
        service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(new StatMonitor.statBackLoop(), 0, 10, TimeUnit.SECONDS);
    }

    private HashMap<String, TSRecord> gatherStatistics(){
        lock.readLock().lock();
        HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
        for (Map.Entry<String, StatProcessor> entry : registProcessor.entrySet()) {
            tsRecordHashMap.putAll(entry.getValue().getStatistics());
        }
        lock.readLock().unlock();
        return tsRecordHashMap;
    }


    private void insert(HashMap<String, TSRecord> tsRecordHashMap) {
        for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
            try {
                FileNodeManager.getInstance().insert(entry.getValue());
            } catch (FileNodeManagerException e) {
                LOGGER.debug(entry.getValue().dataPointList.toString());
                LOGGER.debug("Insert Stat Points error!");
            }
        }
    }

    private void close() {
        service.shutdown();
    }

}