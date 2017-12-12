package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

import java.util.HashMap;


public interface StatProcessor {
    /**
     * @return An HashMap that contains the statistics measurement and its value as TSRecord format.
     */
    HashMap<String, TSRecord> getStatistics();

    void registStatMetadata();
}
