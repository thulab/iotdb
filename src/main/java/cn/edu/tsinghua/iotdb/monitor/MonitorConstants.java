package cn.edu.tsinghua.iotdb.monitor;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MonitorConstants {
    public static final String FILENODE_PROCESSOR_CONST = "FILENODE_PROCESSOR_CONST";
    public static final String FILENODE_MANAGER_CONST = "FILENODE_MANAGER_CONST";
    public static final String MONITOR_PATH_SEPERATOR = ".";
    public static String getStatPrefix() {
        return statPrefix;
    }

    public static final String statPrefix = "root.stats";

    public static HashMap<String, AtomicLong> iniValues(String constantsType) {
        HashMap<String, AtomicLong> hashMap = new HashMap<>();
        switch (constantsType) {
            case FILENODE_PROCESSOR_CONST:
                for (FileNodeProcessorStatConstants c : FileNodeProcessorStatConstants.values()) {
                    hashMap.put(c.name(), new AtomicLong(0));
                }
                break;
            case FILENODE_MANAGER_CONST:
                for (FileNodeManagerStatConstants c : FileNodeManagerStatConstants.values()) {
                    hashMap.put(c.name(), new AtomicLong(0));
                }
                break;
            default:
                //TODO: throws some errors
                break;
        }
        return hashMap;
    }

    public enum FileNodeManagerStatConstants {
        TOTAL_POINTS, TOTAL_REQ_SUCCESS, TOTAL_REQ_FAIL,
        TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL,

    }

    public enum FileNodeProcessorStatConstants {
        TOTAL_REQ_SUCCESS, TOTAL_REQ_FAIL,
        TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL,
    }
}
