package cn.edu.tsinghua.iotdb.engine.tombstone;

import cn.edu.tsinghua.iotdb.concurrent.IoTThreadFactory;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class TombstoneMergeManager extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneMergeManager.class);

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private ExecutorService mergePool = Executors.newFixedThreadPool(config.maxTombstoneThread, new IoTThreadFactory("TombstoneMergePool"));

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(config.tombstoneMergeInterval);
            } catch (InterruptedException e) {
                LOGGER.info("Tombstone merge manager exits...");
                return;
            }
            if(this.isInterrupted()){
                LOGGER.info("Tombstone merge manager exits...");
                return;
            }
            if(((ThreadPoolExecutor) mergePool).getActiveCount() >= 0.5 * config.maxTombstoneThread) {
                LOGGER.info("Half of last tombstone merges are ongoing, wait for their completion.");
            }
            List<TombstoneMergeTask> mergeTaskList;
            try {
                mergeTaskList = FileNodeManager.getInstance().getTombstoneMergeTasks();
            } catch (IOException e) {
                LOGGER.error("Cannot get tombstone merge tasks because {}", e.getMessage());
                continue;
            }
            for(TombstoneMergeTask task : mergeTaskList) {
                mergePool.submit(task);
            }
        }
    }
}
