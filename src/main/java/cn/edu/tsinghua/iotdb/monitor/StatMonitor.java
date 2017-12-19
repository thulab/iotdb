package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    // key is the store path like FileNodeProcessor.root_stats_xxx.xxx
    // or simple name like:FileNodeManager
    private HashMap<String, IStatistic> registProcessor;
    private ScheduledExecutorService service;

    /**
     * stats params
     */
    private AtomicLong numBackLoop = new AtomicLong(0);
    private AtomicLong numBackLoopError = new AtomicLong(0);
    private AtomicLong numInsert = new AtomicLong(0);
    private AtomicLong numPointsInsert = new AtomicLong(0);

    private StatMonitor() {
        mManager = MManager.getInstance();
        lock = new ReentrantReadWriteLock();
        registProcessor = new HashMap<>();
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        backLoopPeriod = config.backLoopPeriod;
        try {
            if (!mManager.pathExist("root.stats")) {
                mManager.setStorageLevelToMTree("root.stats");
            }
            registProcessor.clear();
            // Add all the fileNode to registProcessor
//            List<String> fileNames = mManager.getAllFileNames();
//            for (String fileName : fileNames) {
//                String statPrefix = MonitorConstants.getStatPrefix();
//                String statFilePath = statPrefix + "write." + fileName.replaceAll("\\.", "_");
//                System.out.println(statFilePath);
//                if (!MManager.getInstance().pathExist(statFilePath)) {
//                    mManager.addPathToMTree(
//                            statFilePath, "INT64", "RLE", new String[0]
//                    );
//                }
//                registProcessor.put(statFilePath, null);
//            }
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

    public Long getNumPointsInsert() {
        return numPointsInsert.get();
    }

    public Long getNumInsert() {
        return numInsert.get();
    }

    public void activate() {
        LOGGER.debug("activate!");
        service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(new StatMonitor.statBackLoop(),
                1, backLoopPeriod, TimeUnit.SECONDS
        );
    }

    public void clearProcessor() {
        registProcessor.clear();
    }

    public Long getNumBackLoop() {
        return numBackLoop.get();
    }

    public Long getNumBackLoopError() {
        return numBackLoopError.get();
    }

    public synchronized void registStatistics(String path, IStatistic statprocessor) {
        LOGGER.debug("StatMonitor is in registStatistics:" + path);
        registProcessor.put(path, statprocessor);
    }

    public synchronized void registStatMetadata(HashMap<String, String> hashMap) {
        try {
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                if (entry.getKey() == null) {
                    LOGGER.error("!!!!!!!!!!!!!!!!!!!!!");
                    LOGGER.error(entry.getKey());
                }

                if (!mManager.pathExist(entry.getKey())) {
                    mManager.addPathToMTree(
                            entry.getKey(), entry.getValue(), "RLE", new String[0]);
                }
            }
        } catch (Exception e) {
            LOGGER.error("initialize the metadata error, stat processor may be wrong");
        }
    }

    public synchronized void deregistStatistics(String path) {
        if (registProcessor.containsKey(path)) {
            registProcessor.put(path, null);
        }
    }

    /**
     * TODO: need to complete the query key concept
     *
     * @param key
     * @return TSRecord, query statistics params
     */
    public HashMap<String, TSRecord> getOneStatisticsValue(String key) {
        // queryPath like fileNode path: root.stats.car1, or FileNodeManager path: FileNodeManager
        String queryPath;
        if (key.contains("\\.")) {
            queryPath = MonitorConstants.getStatPrefix() + key.replaceAll("\\.", "_");
        } else {
            queryPath = key;
        }
        if (registProcessor.containsKey(queryPath)) {
            return registProcessor.get(queryPath).getAllStatisticsValue();
        } else {
            Long t = System.currentTimeMillis();
            HashMap<String, TSRecord> hashMap = new HashMap<>();
            TSRecord tsRecord = convertToTSRecord(
                    MonitorConstants.iniValues("FileNodeProcessorStatConstants"),
                    queryPath,
                    t
            );
            hashMap.put(queryPath, tsRecord);
            return hashMap;
        }
    }

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
        FileNodeManager fManager = FileNodeManager.getInstance();
        int count = 0;
        int pointNum;
        for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
            try {
                fManager.insert(entry.getValue());
                numInsert.incrementAndGet();
                LOGGER.debug("insert entry.getValue():" + entry.getValue());
                pointNum = entry.getValue().dataPointList.size();
                numPointsInsert.addAndGet(pointNum);
                count += pointNum;
            } catch (FileNodeManagerException e) {
                LOGGER.debug(entry.getValue().dataPointList.toString());
                LOGGER.debug("Insert Stat Points error!");
            }
        }
        LOGGER.debug("Now StatMonitor is inserting " + count + " points, " +
                "and the FileNodeManager is " + FileNodeManager.getInstance().getStatParamsHashMap().
                get(MonitorConstants.FileNodeManagerStatConstants.TotalPoints.name()));
    }

    public void close() {
        registProcessor.clear();
        if (service == null || service.isShutdown()) {
            return;
        }

        service.shutdown();
        try {
            service.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("StatMonitor timing service could not be shutdown");
        }
    }

    private static class StatMonitorHolder {
        private static final StatMonitor INSTANCE = new StatMonitor();
    }

    class statBackLoop implements Runnable {
        public void run() {
            try {
                LOGGER.debug("---------This is the Time to monitor--------------");
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