package cn.edu.tsinghua.iotdb.monitor;

public class MonitorConstants {
    public static final String DataType = "INT64";
    public static final String STATISTIC_PATH_SEPERATOR = ".";
    public static final String STORAGEGROUP_PATH_SEPERATOR = ".";
    public static final String statStorageGroupPrefix = "root.stats";
    public static final String StorageGroupPrefix = "root";

    public static String convertStorageGroupPathToStatisticPath(String path){
        if(path.equals("root"))return "root.stats";

        path = path.substring((StorageGroupPrefix + STORAGEGROUP_PATH_SEPERATOR).length());
        path = statStorageGroupPrefix + STATISTIC_PATH_SEPERATOR + path;
        return path;
    }

    public static String convertStatisticPathToStorageGroupPath(String path){
        path = path.substring(path.lastIndexOf(statStorageGroupPrefix + STATISTIC_PATH_SEPERATOR));
        path += StorageGroupPrefix + STORAGEGROUP_PATH_SEPERATOR + path;
        return path;
    }

    public enum StatisticConstants {
        TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL,
    }
}
