package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
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
public class StatMonitor implements IStatistic {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatMonitor.class);
    private HashMap<String, IStatistic> registProcessor;

    // Design for mutual access service
    private final ReadWriteLock lock;
    private ScheduledExecutorService service;
    private final int backLoopPeriod;

    public Long getNumBackLoop() {
        return numBackLoop.get();
    }

    public Long getNumBackLoopError() {
        return numBackLoopError.get();
    }

    /**
     * stats params
     */
    private AtomicLong numBackLoop = new AtomicLong(0);
    private AtomicLong numBackLoopError = new AtomicLong(0);
    private AtomicLong numPointInsert = new AtomicLong(0);


    class statBackLoop implements Runnable {
        public void run() {
            try {
                HashMap<String, TSRecord> tsRecordHashMap = gatherStatistics();
                insert(tsRecordHashMap);
                numBackLoop.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
                numBackLoopError.incrementAndGet();
            }
        }
    }

    public void activate(){
        service.scheduleAtFixedRate(new StatMonitor.statBackLoop(),
                0, backLoopPeriod, TimeUnit.SECONDS
        );
    }

    public synchronized void registStatistics(String path, IStatistic statprocessor){
        registProcessor.put(path, statprocessor);
    }

    public synchronized void registStatMetadata(HashMap<String, String> hashMap) {
        try {
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                MManager.getInstance().addPathToMTree(
                        entry.getKey(), entry.getValue(), "RLE", new String[0]);
            }
        } catch (Exception e) {
            LOGGER.debug("initialize the metadata error, stat processor may be wrong");
        }
    }

    public synchronized void deregistStatistics(String path){
        registProcessor.remove(path);
    }

    private StatMonitor(){
        try {
            MManager.getInstance().setStorageLevelToMTree("root.statistics");
        } catch (PathErrorException | IOException e) {
            LOGGER.error("MManager.getInstance().setStorageLevelToMTree False");
        }
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        lock = new ReentrantReadWriteLock();
        registProcessor = new HashMap<>();
        service = Executors.newScheduledThreadPool(1);
        backLoopPeriod = config.backLoopPeriod;
    }

    public static StatMonitor getInstance(){
        return StatMonitorHolder.INSTANCE;
    }
    private static class StatMonitorHolder{
        private static final StatMonitor INSTANCE = new StatMonitor();
    }

    @Override
    public HashMap<String, TSRecord> getAllStatisticsValue() {
        // TODO: need other stats
        return gatherStatistics();
    }

    @Override
    public void registStatMetadata() {

    }

    private HashMap<String, TSRecord> gatherStatistics(){
        lock.readLock().lock();
        HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
        for (Map.Entry<String, IStatistic> entry : registProcessor.entrySet()) {
            tsRecordHashMap.putAll(entry.getValue().getAllStatisticsValue());
        }
        lock.readLock().unlock();
        return tsRecordHashMap;
    }

    private void insert(HashMap<String, TSRecord> tsRecordHashMap) {
        for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
            try {
                FileNodeManager.getInstance().insert(entry.getValue());
                numPointInsert.addAndGet(entry.getValue().dataPointList.size());
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