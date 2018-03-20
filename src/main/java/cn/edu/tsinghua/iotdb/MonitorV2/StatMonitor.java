package cn.edu.tsinghua.iotdb.MonitorV2;

import cn.edu.tsinghua.iotdb.MonitorV2.Event.StatEvent;
import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.concurrent.ThreadName;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.service.IService;
import cn.edu.tsinghua.iotdb.service.ServiceType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
        if(event == null)return;

        StatisticTSRecord record = event.convertToStatTSRecord();
        updateStatisticMap(record);
        updateStatisticInDB();
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

            path = path.substring(0, path.lastIndexOf(MonitorConstants.MONITOR_PATH_SEPERATOR));
        }while (path.contains(MonitorConstants.MONITOR_PATH_SEPERATOR));
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
        statistics.clear();
    }

    @Override
    public ServiceType getID() {
        return ServiceType.STAT_MONITOR_SERVICE;
    }

    class statBackLoop implements Runnable {
        public void run() {
            while(true){
                while (events.isEmpty()) try {
                    Thread.sleep(10000);
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

    public static void main(String[] args){
        StatisticTSRecord record = new StatisticTSRecord(1000, "root");
        StatisticTSRecord record1 = new StatisticTSRecord(record, "root1");
        System.out.println(record);
        System.out.println(record1);
        record.addOneStatistic(StatisticTSRecord.StatisticConstants.TOTAL_POINTS_SUCCESS, 100);
        System.out.println(record);
        System.out.println(record1);
    }
}
