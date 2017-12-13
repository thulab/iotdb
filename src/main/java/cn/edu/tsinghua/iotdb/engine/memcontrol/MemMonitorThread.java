package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;

public class MemMonitorThread extends Thread {

    private long checkInterval = 1000; // in ms

    public MemMonitorThread(long checkInterval) {
        this.checkInterval = checkInterval > 0 ? checkInterval : this.checkInterval;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            MemController.UsageLevel level = MemController.getInstance().getCurrLevel();
            switch (level) {
                case WARNING:
                case DANGEROUS:
                    FileNodeManager.getInstance().forceFlush(level);
                case SAFE:
                default:
            }
            try {
                Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
