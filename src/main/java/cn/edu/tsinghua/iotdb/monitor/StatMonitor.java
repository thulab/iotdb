package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    // Design for mutual access service
    private final ReadWriteLock lock;
    private final int backLoopPeriod;
    private final MManager mManager;

    // key is the store path like root.stats.xxx.xxx, value is an interface
    private HashMap<String, IStatistic> registProcessor;
    private ScheduledExecutorService service;
    /**
     * stats params
     */
    private AtomicLong numBackLoop = new AtomicLong(0);
    private AtomicLong numBackLoopError = new AtomicLong(0);
    private AtomicLong numPointInsert = new AtomicLong(0);

    private StatMonitor() {
        mManager = MManager.getInstance();
        lock = new ReentrantReadWriteLock();
        registProcessor = new HashMap<>();
        service = Executors.newScheduledThreadPool(1);
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        backLoopPeriod = config.backLoopPeriod;
        try {
            if (!mManager.pathExist("root.\\_stats")) {
                mManager.setStorageLevelToMTree("root.\\_stats");
            }
            List<String> fileNames = mManager.getAllFileNames();
            for (String fileName : fileNames) {
                String statPrefix = MonitorConstants.getStatPrefix();
                String statFilePath = statPrefix + "write." + statPrefix.replaceAll(".", "_");
                if (!MManager.getInstance().pathExist(statFilePath)) {
                    mManager.addPathToMTree(
                            statFilePath, "INT64", "RLE", new String[0]
                    );
                }
                registProcessor.put(statFilePath, null);
            }
        } catch (Exception e) {
            LOGGER.error("MManager.getInstance().setStorageLevelToMTree False");
        }
    }

    public static StatMonitor getInstance() {
        return StatMonitorHolder.INSTANCE;
    }

    /**
     * @param hashMap       key is statParams name, values is AtomicLong type
     * @param fakeDeltaName is the deltaObject path of this module
     * @param curTime       TODO need to be fixed may contains overflow
     * @return TSRecord contains the DataPoints of a fakeDeltaName
     */
    public static TSRecord convertToTSRecord(HashMap<String, AtomicLong> hashMap, String fakeDeltaName, Long curTime) {
        TSRecord tsRecord = new TSRecord(curTime, fakeDeltaName);
        tsRecord.dataPointList = new ArrayList<DataPoint>() {{
            for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
                AtomicLong value = (AtomicLong) entry.getValue();
                add(new LongDataPoint(entry.getKey(), value.get()));
            }
        }};
        return tsRecord;
    }

    public Long getNumBackLoop() {
        return numBackLoop.get();
    }

    public Long getNumBackLoopError() {
        return numBackLoopError.get();
    }

    public void activate() {
        service.scheduleAtFixedRate(new StatMonitor.statBackLoop(),
                0, backLoopPeriod, TimeUnit.SECONDS
        );
    }

    public synchronized void registStatistics(String path, IStatistic statprocessor) {
        registProcessor.put(path, statprocessor);
    }

    public synchronized void registStatMetadata(HashMap<String, String> hashMap) {
        try {
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                if (!mManager.pathExist(entry.getKey())) {
                    mManager.addPathToMTree(
                            entry.getKey(), entry.getValue(), "RLE", new String[0]);
                }
            }
        } catch (Exception e) {
            LOGGER.debug("initialize the metadata error, stat processor may be wrong");
        }
    }

    public synchronized void deregistStatistics(String path) {
        registProcessor.remove(path);
    }

    /**
     * TODO: need to complete
     *
     * @param key
     * @return
     */
//    public TSRecord getOneStatisticsValue(String key) {
//        String queryPath = MonitorConstants.getStatPrefix() +
//        if (registProcessor.containsKey(key)) {
//            return registProcessor.get(key).getAllStatisticsValue().get(key);
//        }
//        else{
//
//        }
//    }

    public HashMap<String, TSRecord> gatherStatistics() {
        lock.readLock().lock();
        Long t = System.currentTimeMillis();
        HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
        for (Map.Entry<String, IStatistic> entry : registProcessor.entrySet()) {
            if (entry.getValue() == null) {
                tsRecordHashMap.put("",
                        convertToTSRecord(
                                MonitorConstants.iniValues("FileNodeProcessorStatConstants"),
                                entry.getKey(),
                                t
                        )
                );
            } else {
                tsRecordHashMap.putAll(entry.getValue().getAllStatisticsValue());
            }
        }

        //TODO: need to reset the time information?
        for (String key : tsRecordHashMap.keySet()) {
            tsRecordHashMap.get(key).time = t;
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

    public void close() {
        service.shutdown();
    }

    private static class StatMonitorHolder {
        private static final StatMonitor INSTANCE = new StatMonitor();
    }

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

}