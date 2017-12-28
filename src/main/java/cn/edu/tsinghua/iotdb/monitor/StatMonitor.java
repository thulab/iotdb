package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private final int backLoopPeriod;

    // key is the store path like FileNodeProcessor.root_stats_xxx.xxx,
    // or simple name like:FileNodeManager. And value is interface implement
    // statistics function
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
        MManager mManager = MManager.getInstance();
        registProcessor = new HashMap<>();
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        backLoopPeriod = config.backLoopPeriod;
        try {
            String prefix = MonitorConstants.getStatPrefix();

            if (!mManager.pathExist(prefix)) {
                mManager.setStorageLevelToMTree(prefix);
            }
        } catch (Exception e) {
            LOGGER.error("MManager setStorageLevelToMTree False, {}", e.getMessage());
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

    public void registMeta() {
        MManager mManager = MManager.getInstance();
        String prefix = MonitorConstants.getStatPrefix();
        try {
            if (!mManager.pathExist(prefix)) {
                mManager.setStorageLevelToMTree(prefix);
            }
        } catch (Exception e){
            LOGGER.error("MManager setStorageLevelToMTree False, Because {}", e.getMessage());
        }
    }

    public void activate() {
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

    public void registStatistics(String path, IStatistic statprocessor) {
        synchronized (registProcessor) {
            LOGGER.debug("StatMonitor is in registStatistics: {}", path);
            registProcessor.put(path, statprocessor);
        }
    }

    public synchronized void registMeta(HashMap<String, String> hashMap) {
        MManager mManager = MManager.getInstance();
        try {
            for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                if (entry.getKey() == null) {
                    LOGGER.error("Registering MetaData, {} is null",  entry.getKey());
                }

                if (!mManager.pathExist(entry.getKey())) {
                    mManager.addPathToMTree(
                            entry.getKey(), entry.getValue(), "RLE", new String[0]);
                }
            }
        } catch (MetadataArgsErrorException|IOException|PathErrorException e) {
            LOGGER.error("initialize the metadata error, Because {}", e.getMessage());
        }
    }

    public void deregistStatistics(String path) {
        synchronized (registProcessor) {
            if (registProcessor.containsKey(path)) {
                registProcessor.put(path, null);
            }
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
            queryPath = MonitorConstants.getStatPrefix()
                    + MonitorConstants.MONITOR_PATH_SEPERATOR
                    + key.replaceAll("\\.", "_");
        } else {
            queryPath = key;
        }
        if (registProcessor.containsKey(queryPath)) {
            return registProcessor.get(queryPath).getAllStatisticsValue();
        } else {
            Long currentTimeMillis = System.currentTimeMillis();
            HashMap<String, TSRecord> hashMap = new HashMap<>();
            TSRecord tsRecord = convertToTSRecord(
                    MonitorConstants.iniValues("FileNodeProcessorStatConstants"),
                    queryPath,
                    currentTimeMillis
            );
            hashMap.put(queryPath, tsRecord);
            return hashMap;
        }
    }

    public HashMap<String, TSRecord> gatherStatistics() {
        synchronized (registProcessor) {
            Long currentTimeMillis = System.currentTimeMillis();
            HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
            for (Map.Entry<String, IStatistic> entry : registProcessor.entrySet()) {
                if (entry.getValue() == null) {
                    tsRecordHashMap.put(entry.getKey(),
                            convertToTSRecord(
                                    MonitorConstants.iniValues(MonitorConstants.FILENODE_PROCESSOR_CONST),
                                    entry.getKey(),
                                    currentTimeMillis
                            )
                    );
                } else {
                    tsRecordHashMap.putAll(entry.getValue().getAllStatisticsValue());
                }
            }
            LOGGER.debug("Values of tsRecordHashMap is : {}", tsRecordHashMap.toString());
            for (TSRecord value : tsRecordHashMap.values()) {
                value.time = currentTimeMillis;
            }
            return tsRecordHashMap;
        }
    }

    private void insert(HashMap<String, TSRecord> tsRecordHashMap) {
        FileNodeManager fManager = FileNodeManager.getInstance();
        int count = 0;
        int pointNum;
        for (Map.Entry<String, TSRecord> entry : tsRecordHashMap.entrySet()) {
            try {
                fManager.insert(entry.getValue());
                numInsert.incrementAndGet();
                pointNum = entry.getValue().dataPointList.size();
                numPointsInsert.addAndGet(pointNum);
                count += pointNum;
            } catch (FileNodeManagerException e) {
                LOGGER.error("Inserting Stat Points error, {}",  e.getMessage());
            }
        }
        LOGGER.debug("Now StatMonitor is inserting {} points, " +
                "and the FileNodeManager is {}", count, fManager.getStatParamsHashMap().
                get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS.name()));
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
            LOGGER.error("StatMonitor timing service could not be shutdown with info:{}", e.getMessage());
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