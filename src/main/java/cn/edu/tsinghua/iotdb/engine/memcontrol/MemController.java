package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MemController {

    private static Logger logger = LoggerFactory.getLogger(MemController.class);

    // the key is the reference of the memory user, while the value is its memory usage in byte
    private Map<Object, Long> memMap;
    private AtomicLong totMemUsed;

    private long waringThreshold;
    private long dangerouseThreshold;

    public enum UsageLevel {
        SAFE, WARNING, DANGEROUS
    }

    private static class InstanceHolder {
        private static final MemController INSTANCE = new MemController(TsfileDBDescriptor.getInstance().getConfig());
    }

    private MemController(TsfileDBConfig config) {
        memMap = new HashMap<>();
        totMemUsed = new AtomicLong(0);
        waringThreshold = config.memThresholdWarning;
        dangerouseThreshold = config.memThresholdDangerous;
    }

    public static MemController getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public long getTotUsage() {
        return totMemUsed.get();
    }

    public UsageLevel reportUse(Object user, long usage) {
        Long oldUsage = memMap.get(user);
        if(oldUsage == null)
            oldUsage = 0L;
        long newTotUsage = totMemUsed.get() + usage ;
        // check if the new usage will reach dangerous threshold
        if(newTotUsage < dangerouseThreshold) {
            newTotUsage = totMemUsed.addAndGet(usage);
            // double check if updating will reach dangerous threshold
            if(newTotUsage < waringThreshold) {
                // still safe, action taken
                memMap.put(user, oldUsage + usage);
                return UsageLevel.SAFE;
            } else if(newTotUsage < dangerouseThreshold) {
                // become warning because competition with other threads, still take the action
                memMap.put(user, oldUsage + usage);
                return UsageLevel.WARNING;
            } else {
                // become dangerous because competition with other threads, discard this action
                totMemUsed.addAndGet(-usage);
                return UsageLevel.DANGEROUS;
            }
        } else {
            return UsageLevel.DANGEROUS;
        }
    }

    public void reportFree(Object user, long freeSize) {
        Long usage = memMap.get(user);
        if (usage == null)
            logger.error("Unregistered memory usage from {}", user.getClass());
        else if(freeSize > usage){
            logger.error("Request to free {} bytes while it only registered {} bytes", freeSize, usage);
            totMemUsed.addAndGet(-usage);
            memMap.put(user, 0L);
        } else {
            totMemUsed.addAndGet(-freeSize);
            memMap.put(user, usage - freeSize);
        }
    }
}
