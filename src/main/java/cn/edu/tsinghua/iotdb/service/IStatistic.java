package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import java.util.HashMap;


public interface IStatistic {
    /**
     * @return An HashMap that contains the statistics measurement and its value as TSRecord format.
     */
    HashMap<String, TSRecord> getAllStatisticsValue();

    void registStatMetadata();
}
