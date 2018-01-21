package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

/**
 * IMemTable is designed to store data points which are not flushed into TsFile yet. An instance of IMemTable maintains
 * all series belonging to one StorageGroup, corresponding to one FileNodeProcessor.<br>
 * The concurrent control of IMemTable is based on the concurrent control of FileNodeProcessor,
 * i.e., Writing and querying operations have gotten writeLock and readLock respectively.<br>
 *
 * @author Rong Kang
 */
public interface IMemTable {
    Map<String, Map<String, IMemSeries>> getMemTableMap();

    /**
     * check whether the given path is within this memtable.
     *
     * @return true if path is within this memtable
     *
     */
    boolean checkPath(String deltaObject, String measurement);

    IMemSeries addSeriesIfNone(String deltaObject, String measurement, TSDataType dataType);

    public void write(String deltaObject, String measurement, TSDataType dataType, long insertTime, String insertValue);

    int size();

    Iterable<?> query(String deltaObject, String measurement,TSDataType dataType);

    void resetMemSeries(String deltaObject, String measurement);
    
    void clear();
    
    boolean isEmpty();

}

