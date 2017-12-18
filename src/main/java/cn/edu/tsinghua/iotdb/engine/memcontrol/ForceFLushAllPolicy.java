package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForceFLushAllPolicy implements Policy {
    private Logger logger = LoggerFactory.getLogger(ForceFLushAllPolicy.class);

    @Override
    public void execute() {
        logger.info("Memory reachs {}, current memory size is {}, flushing.",
                MemController.getInstance().getCurrLevel(),
                MemUtils.bytesCntToStr(MemController.getInstance().getTotalUsage()));
        // TODO : fix : partial flush may always flush the same filenodes
        FileNodeManager.getInstance().forceFlush(MemController.UsageLevel.DANGEROUS);
        logger.info("Flush over, current memory size is {}", MemUtils.bytesCntToStr(MemController.getInstance().getTotalUsage()));
    }
}
