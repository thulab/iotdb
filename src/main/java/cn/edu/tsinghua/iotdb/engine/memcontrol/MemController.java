package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class hold global memory usage of MemUsers.
 */
public class MemController {

    private static Logger logger = LoggerFactory.getLogger(MemController.class);

    // the key is the reference of the memory user, while the value is its memory usage in byte
    private Map<Object, Long> memMap;
    private AtomicLong totalMemUsed;

    private long waringThreshold;
    private long dangerouseThreshold;

    private MemMonitorThread monitorThread;

    public enum UsageLevel {
        SAFE, WARNING, DANGEROUS
    }

    private static class InstanceHolder {
        private static final MemController INSTANCE = new MemController(TsfileDBDescriptor.getInstance().getConfig());
    }

    private MemController(TsfileDBConfig config) {
        memMap = new HashMap<>();
        totalMemUsed = new AtomicLong(0);
        waringThreshold = config.memThresholdWarning;
        dangerouseThreshold = config.memThresholdDangerous;

        monitorThread = new MemMonitorThread(config.memMonitorInterval);
        monitorThread.start();
    }

    public static MemController getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public long getTotalUsage() {
        return totalMemUsed.get();
    }

    public void clear() {
        memMap.clear();
        totalMemUsed.set(0);
    }

    public void close() {
        monitorThread.interrupt();
    }

    public UsageLevel getCurrLevel() {
        long memUsage = totalMemUsed.get();
        if(memUsage < waringThreshold){
            return UsageLevel.SAFE;
        } else if(memUsage < dangerouseThreshold) {
            return UsageLevel.WARNING;
        } else {
            return  UsageLevel.DANGEROUS;
        }
    }

    public UsageLevel reportUse(Object user, long usage) {
        Long oldUsage = memMap.get(user);
        if(oldUsage == null)
            oldUsage = 0L;
        long newTotUsage = totalMemUsed.get() + usage ;
        // check if the new usage will reach dangerous threshold
        if(newTotUsage < dangerouseThreshold) {
            newTotUsage = totalMemUsed.addAndGet(usage);
            // double check if updating will reach dangerous threshold
            if(newTotUsage < waringThreshold) {
                // still safe, action taken
                memMap.put(user, oldUsage + usage);
                logger.debug("Memory allocated to {}, it is using {} GB {} MB {} B, total usage {} GB {} MB {} B",user.getClass()
                        , (oldUsage + usage) / TsFileDBConstant.GB, (oldUsage + usage) / TsFileDBConstant.MB % 1024, (oldUsage + usage) % TsFileDBConstant.MB
                        , newTotUsage / TsFileDBConstant.GB, newTotUsage / TsFileDBConstant.MB % 1024, newTotUsage % TsFileDBConstant.MB);
                return UsageLevel.SAFE;
            } else if(newTotUsage < dangerouseThreshold) {
                // become warning because competition with other threads, still take the action
                memMap.put(user, oldUsage + usage);
                logger.debug("Memory allocated to {}, it is using {} GB {} MB {} B, total usage {} GB {} MB {} B",user.getClass()
                        , (oldUsage + usage) / TsFileDBConstant.GB, (oldUsage + usage) / TsFileDBConstant.MB % 1024, (oldUsage + usage) % TsFileDBConstant.MB
                        , newTotUsage / TsFileDBConstant.GB, newTotUsage / TsFileDBConstant.MB % 1024, newTotUsage % TsFileDBConstant.MB);
                return UsageLevel.WARNING;
            } else {
                // become dangerous because competition with other threads, discard this action
                totalMemUsed.addAndGet(-usage);
                return UsageLevel.DANGEROUS;
            }
        } else {
            return UsageLevel.DANGEROUS;
        }
    }

    public void reportFree(Object user, long freeSize) {
        Long usage = memMap.get(user);
        if (usage == null)
            logger.debug("Unregistered memory usage from {}", user.getClass());
        else if(freeSize > usage){
            logger.debug("Request to free {} bytes while it only registered {} bytes", freeSize, usage);
            totalMemUsed.addAndGet(-usage);
            memMap.put(user, 0L);
        } else {
            long newTotalMemUsage = totalMemUsed.addAndGet(-freeSize);
            memMap.put(user, usage - freeSize);
            logger.debug("Memory freed from {}, it is using {} GB {} MB, total usage {} GB {} MB",user.getClass()
                    , (usage - freeSize) / TsFileDBConstant.GB, (usage - freeSize) / TsFileDBConstant.MB % 1000
                    , newTotalMemUsage / TsFileDBConstant.GB, newTotalMemUsage / TsFileDBConstant.MB % 1000);
        }
    }
}
