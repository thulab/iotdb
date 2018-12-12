package cn.edu.tsinghua.iotdb.queryV2.dataset;

import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class EngineDataSetWithoutTimeGenerator extends QueryDataSet {

  private List<IReader> readers;

  private List<BatchData> batchDataList;

  private List<Boolean> hasDataRemaining;

  // heap only need to store time
  private PriorityQueue<Long> timeHeap;

  private Set<Long> timeSet;

  public EngineDataSetWithoutTimeGenerator(List<Path> paths, List<TSDataType> dataTypes, List<IReader> readers) throws IOException {
    super(paths, dataTypes);
    this.readers = readers;
    //initHeap();
  }

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }

  @Override
  public RowRecord next() throws IOException {
    return null;
  }
}
