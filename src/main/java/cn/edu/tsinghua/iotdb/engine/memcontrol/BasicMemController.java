package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;

public abstract class BasicMemController {

    protected long warningThreshold;
    protected long dangerouseThreshold;

    protected MemMonitorThread monitorThread;

    public enum UsageLevel {
        SAFE, WARNING, DANGEROUS
    }

    BasicMemController(TsfileDBConfig config) {
        warningThreshold = config.memThresholdWarning;
        dangerouseThreshold = config.memThresholdDangerous;
        monitorThread = new MemMonitorThread(config.memMonitorInterval);
        monitorThread.start();
    }

    // change instance here
    public static BasicMemController getInstance() {
        return RecordMemController.getInstance();
    }

    public void setDangerouseThreshold(long dangerouseThreshold) {
        this.dangerouseThreshold = dangerouseThreshold;
    }

    public void setWarningThreshold(long warningThreshold) {
        this.warningThreshold = warningThreshold;
    }

    public void setCheckInterval(long checkInterval) {
        this.monitorThread.setCheckInterval(checkInterval);
    }

    public abstract long getTotalUsage();

    public abstract UsageLevel getCurrLevel();

    public abstract void clear();

    public abstract void close();

    public abstract UsageLevel reportUse(Object user, long usage);

    public abstract void reportFree(Object user, long freeSize);
}
