package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import java.util.HashMap;
import java.util.List;


public interface IStatistic {
    /**
     * @return An HashMap that contains the statistics measurement and its value as TSRecord format.
     */
    HashMap<String, TSRecord> getAllStatisticsValue();

    void registStatMetadata();

    /**
     *
     * @return a list of string like "root.statistics.xxx.xxx."
     */
    public List<String> getAllPathForStatistic();
}
