package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;

public interface IFixStatistics {

    void registStatStorageGroup(FileNodeProcessor fileNodeProcessor);

    void fixStatistics();
}
