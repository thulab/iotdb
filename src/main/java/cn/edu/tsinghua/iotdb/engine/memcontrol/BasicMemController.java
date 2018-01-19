package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.service2.IService;
import cn.edu.tsinghua.iotdb.service2.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicMemController implements IService{

    private static final Logger logger = LoggerFactory.getLogger(BasicMemController.class);
    private TsfileDBConfig config;

    @Override
    public void start() {
        if(config.enableMemMonitor) {
            if(monitorThread == null) {
                monitorThread = new MemMonitorThread(config);
                monitorThread.start();
            } else {
                logger.error("Attempt to start MemController but it is already started");
            }
            if(memStatisticThread == null) {
                memStatisticThread = new MemStatisticThread();
                memStatisticThread.start();
            } else {
                logger.error("Attempt to start MemController but it is already started");
            }
        }
        logger.info("MemController starts");
    }

    @Override
    public void stop() {
        clear();
        close();
    }

    @Override
    public ServiceType getID() {
        return ServiceType.JVM_MEM_CONTROL_SERVICE;
    }

    public enum CONTROLLER_TYPE {
        RECORD, JVM
    }

    protected long warningThreshold;
    protected long dangerouseThreshold;

    protected MemMonitorThread monitorThread;
    protected MemStatisticThread memStatisticThread;

    public enum UsageLevel {
        SAFE, WARNING, DANGEROUS
    }

    BasicMemController(TsfileDBConfig config) {
        this.config = config;
        warningThreshold = config.memThresholdWarning;
        dangerouseThreshold = config.memThresholdDangerous;
    }

    // change instance here
    public static BasicMemController getInstance() {
        switch (CONTROLLER_TYPE.values()[TsfileDBDescriptor.getInstance().getConfig().memControllerType]) {
            case JVM:
                return JVMMemController.getInstance();
            case RECORD:
            default:
                return RecordMemController.getInstance();
        }
    }

    public void setDangerouseThreshold(long dangerouseThreshold) {
        this.dangerouseThreshold = dangerouseThreshold;
    }

    public void setWarningThreshold(long warningThreshold) {
        this.warningThreshold = warningThreshold;
    }

    public void setCheckInterval(long checkInterval) {
        if(this.monitorThread != null)
            this.monitorThread.setCheckInterval(checkInterval);
    }

    public abstract long getTotalUsage();

    public abstract UsageLevel getCurrLevel();

    public abstract void clear();

    public void close() {
        logger.info("MemController exiting");
        if(monitorThread != null) {
            monitorThread.interrupt();
            while (monitorThread.isAlive()) {
            }
            monitorThread = null;
        }

        if(memStatisticThread != null) {
            memStatisticThread.interrupt();
            while (memStatisticThread.isAlive()) {
            }
            memStatisticThread = null;
        }
        logger.info("MemController exited");
    }

    public abstract UsageLevel reportUse(Object user, long usage);

    public abstract void reportFree(Object user, long freeSize);
}
