package cn.edu.tsinghua.iotdb.MonitorV2;

import cn.edu.tsinghua.iotdb.MonitorV2.Event.StatEvent;
import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.concurrent.ThreadName;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.impl.QueryEngineImpl;
import cn.edu.tsinghua.iotdb.service.IService;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.ServiceType;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StatMonitor implements StatEventListener, IService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatMonitor.class);

    private Map<String, StatisticTSRecord> statistics;
    private ScheduledExecutorService service;
    private final int backLoopPeriod;
    private final int statMonitorDetectFreqSec;
    private final int statMonitorRetainIntervalSec;

    private Queue<StatEvent> events;

    public StatMonitor(){
        statistics = new HashMap<>();
        events = new ConcurrentLinkedQueue<>();

        MManager mManager = MManager.getInstance();
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        statMonitorDetectFreqSec = config.statMonitorDetectFreqSec;
        statMonitorRetainIntervalSec = config.statMonitorRetainIntervalSec;
        backLoopPeriod = config.backLoopPeriodSec;
        if (config.enableStatMonitor){
            try {
                String prefix = MonitorConstants.statStorageGroupPrefix;
                if (!mManager.pathExist(prefix)) {
                    mManager.setStorageLevelToMTree(prefix);
                }
            } catch (PathErrorException |IOException e) {
                LOGGER.error("MManager cannot set storage level to MTree.", e);
            }
        }
    }

    @Override
    public void addEvent(StatEvent event) {
        events.add(event);
        LOGGER.info(String.format("Receive event from %s, type is %s, value is %s.", event.getPath(), event.getClass().getSimpleName(), event.getValue()));
    }

    @Override
    public void dealWithEvent(StatEvent event) {
        if (event == null)return;
        if (isStatisticPath(event.getPath()))return;

        StatisticTSRecord record = event.convertToStatTSRecord();
        updateStatisticMap(record);
        updateStatisticInDB();
    }

    public void dealWithAllEvents(){
        while (!events.isEmpty()){
            dealWithEvent(events.poll());
        }
    }

    private boolean isStatisticPath(String path){
        return path.startsWith(MonitorConstants.statStorageGroupPrefix);
    }

    private void updateStatisticInDB(){
        FileNodeManager fManager = FileNodeManager.getInstance();
        long current_time = System.currentTimeMillis();
        for (Map.Entry<String, StatisticTSRecord> entry : statistics.entrySet()) {
            try {
                entry.getValue().time = current_time;
                fManager.insert(entry.getValue(), true);
            } catch (FileNodeManagerException e) {
                statistics.get(MonitorConstants.statStorageGroupPrefix).addOneStatistic(StatisticTSRecord.StatisticConstants.TOTAL_REQ_FAIL, 1);
                LOGGER.error("Inserting stat points error.",  e);
            }
        }
    }

    private void updateStatisticMap(StatisticTSRecord record){
        String path = record.deltaObjectId;
        do{
            if(statistics.containsKey(path)){
                statistics.get(path).addOneStatistic(StatisticTSRecord.StatisticConstants.TOTAL_POINTS_SUCCESS, record);
            }else{
                StatisticTSRecord newRecord = new StatisticTSRecord(record, path);
                statistics.put(path, newRecord);
                List<String> paths = StatisticTSRecord.getAllPaths(path);
                MManager mManager = MManager.getInstance();
                for(String p : paths){
                    try {
                        mManager.addPathToMTree(p, "INT64", "RLE", new String[0]);
                    } catch (MetadataArgsErrorException|IOException|PathErrorException e) {
                        LOGGER.error("Adding statistic path error.", e);
                    }
                }
            }

            path = path.substring(0, path.lastIndexOf(MonitorConstants.STATISTIC_PATH_SEPERATOR));
        }while (path.contains(MonitorConstants.STATISTIC_PATH_SEPERATOR));
    }

    public void recovery(){
        try {
            statistics.clear();

            List<String> statPaths = getStatPaths();
            Map<String, Long> res;
            try {
                res = getCurrentStatisticInDB(statPaths);
            }catch (NullPointerException ex){
                return;
            }

            long currenttime = System.currentTimeMillis();
            Map<String, Map<String, Long>> stats = new HashMap<>();
            for(Map.Entry<String, Long> entry : res.entrySet()){
                String path = entry.getKey();
                String deltaobjectID = path.substring(0, path.lastIndexOf(MonitorConstants.STATISTIC_PATH_SEPERATOR));
                String measurementID = path.substring(path.lastIndexOf(MonitorConstants.STATISTIC_PATH_SEPERATOR) + 1);

                if(!stats.containsKey(deltaobjectID)){
                    stats.put(deltaobjectID, new HashMap<>());
                }
                stats.get(deltaobjectID).put(measurementID, entry.getValue());
            }

            for(Map.Entry<String, Map<String, Long>> entry : stats.entrySet()){
                StatisticTSRecord tsRecord = new StatisticTSRecord(currenttime, entry.getKey(), entry.getValue());
                statistics.put(entry.getKey(), tsRecord);
            }
            updateStatisticInDB();
        } catch (PathErrorException e) {
            e.printStackTrace();
        }
    }

    private List<String> getStatPaths() throws PathErrorException {
        List<String> sgList = MManager.getInstance().getAllFileNames();
        Set<String> statPathSet = new HashSet<>();
        List<String> statPaths = new ArrayList<>();
        for(String storagegroup : sgList){
            if(storagegroup.equals(MonitorConstants.statStorageGroupPrefix))continue;
            while (true) {
                if (statPathSet.contains(storagegroup)) break;
                else statPathSet.add(storagegroup);

                String statpath = MonitorConstants.convertStorageGroupPathToStatisticPath(storagegroup);
                for (StatisticTSRecord.StatisticConstants constants : StatisticTSRecord.StatisticConstants.values())
                    statPaths.add(statpath + MonitorConstants.STATISTIC_PATH_SEPERATOR + constants.name());

                if (!storagegroup.contains(MonitorConstants.STORAGEGROUP_PATH_SEPERATOR)) break;
                storagegroup = storagegroup.substring(0, storagegroup.lastIndexOf(MonitorConstants.STORAGEGROUP_PATH_SEPERATOR));
            }
        }
        return statPaths;
    }

    private Map<String, Long> getCurrentStatisticInDB(List<String> paths) throws NullPointerException {
        Map<String, Long> res = new HashMap<>();
        OverflowQueryEngine overflowQueryEngine = new OverflowQueryEngine();
        List<Pair<Path, String>> pairList = new ArrayList<>();
        for (String string : paths) {
            Path path = new Path(string);
            pairList.add(new Pair<>(path, StatisticConstant.LAST));
        }
        try {
            cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet queryDataSet;
            queryDataSet = overflowQueryEngine.aggregate(pairList, null);
            ReadLockManager.getInstance().unlockForOneRequest();
            RowRecord rowRecord = queryDataSet.getNextRecord();

            if (rowRecord!=null) {
                List<Field> list = rowRecord.fields;
                for (Field field: list) {
                    String statDeltaobject = field.deltaObjectId.substring(field.deltaObjectId.indexOf("(") + 1);
                    String statMeasurement = field.measurementId.substring(0,field.measurementId.length() - 1);
                    res.put(statDeltaobject + MonitorConstants.STATISTIC_PATH_SEPERATOR + statMeasurement, field.getLongV());
                }
            }
        } catch (ProcessorException e) {
            LOGGER.error("Can't get the processor when recovering statistics of FileNodeManager,", e);
        } catch (PathErrorException e) {
            LOGGER.error("When recovering statistics of FileNodeManager, timeseries path does not exist,", e);
        } catch (IOException e) {
            LOGGER.error("IO Error occurs when recovering statistics of FileNodeManager,", e);
        }
        return res;
    }

    public static StatMonitor getInstance() {
        return StatMonitorHolder.INSTANCE;
    }

    private static class StatMonitorHolder {
        private static final StatMonitor INSTANCE = new StatMonitor();
    }

    @Override
    public void start() throws StartupException {
        try {
            if (TsfileDBDescriptor.getInstance().getConfig().enableStatMonitor){
                activate();
            }
        } catch (Exception e) {
            String errorMessage = String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
            throw new StartupException(errorMessage);
        }
    }

    public void activate() {
        service = IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.STAT_MONITOR.getName());
        service.scheduleAtFixedRate(new StatMonitor.statBackLoop(),
                1, backLoopPeriod, TimeUnit.SECONDS
        );
    }

    @Override
    public void stop() {
        if (TsfileDBDescriptor.getInstance().getConfig().enableStatMonitor){
            close();
        }
    }

    public void close() {

        if (service == null || service.isShutdown()) {
            return;
        }
        statistics.clear();
        service.shutdown();
        try {
            service.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("StatMonitor timing service could not be shutdown.", e);
        }
    }

    @Override
    public ServiceType getID() {
        return ServiceType.STAT_MONITOR_SERVICE;
    }

    class statBackLoop implements Runnable {
        public void run() {
            while(true){
                while (events.isEmpty()) try {
                    Thread.sleep(backLoopPeriod);
                    continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                StatEvent event = events.poll();
                LOGGER.info(String.format("Start to deal with %s from %s.", event.getClass().getSimpleName(), event.getPath()));
                dealWithEvent(event);
            }
        }
    }
}
