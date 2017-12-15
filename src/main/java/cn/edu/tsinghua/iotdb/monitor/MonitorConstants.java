package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MonitorConstants {
    public static String getStatPrefix() {
        return statPrefix;
    }

    public static final String statPrefix = "root.\\_stats.";

    public static HashMap<String, AtomicLong> iniValues(String constantsType) {
        HashMap<String, AtomicLong> hashMap = new HashMap<>();
        switch (constantsType) {
            case "MonitorConstants":
                for (FileNodeManagerStatConstants c : FileNodeManagerStatConstants.values()) {
                    hashMap.put(c.name(), new AtomicLong(0));
                }
                break;
            case "FileNodeProcessorStatConstants":
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
        TotalPoints, TotalReqSuccess, TotalReqFail,
        TotalPointsSuccess, TotalPointsFail,

    }

    public enum FileNodeProcessorStatConstants {
        TotalReqSuccess, TotalReqFail,
        TotalPointsSuccess, TotalPointsFail,
    }
}
