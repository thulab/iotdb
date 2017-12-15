package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemMonitorThread extends Thread {

    private static Logger logger = LoggerFactory.getLogger(MemMonitorThread.class);

    private long checkInterval = 1000; // in ms

    public MemMonitorThread(long checkInterval) {
        this.checkInterval = checkInterval > 0 ? checkInterval : this.checkInterval;
    }

    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            MemController.UsageLevel level = MemController.getInstance().getCurrLevel();
            switch (level) {
                case WARNING:
                case DANGEROUS:
                    logger.info("Memory reachs {}, current memory size is {}, flushing.",
                            level, MemUtils.bytesCntToStr(MemController.getInstance().getTotalUsage()));
                    FileNodeManager.getInstance().forceFlush(level);
                    logger.info("Flush over, current memory size is {}", MemUtils.bytesCntToStr(MemController.getInstance().getTotalUsage()));
                case SAFE:
                default:
            }
            try {
                Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
                logger.info("MemMonitorThread exiting...");
                return;
            }
        }
    }
}
