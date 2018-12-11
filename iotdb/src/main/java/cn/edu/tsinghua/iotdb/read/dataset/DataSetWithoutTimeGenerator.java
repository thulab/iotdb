package cn.edu.tsinghua.iotdb.read.dataset;

import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class DataSetWithoutTimeGenerator extends QueryDataSet {

  private List<ISeriesReader> readers;

  private List<BatchData> batchDataList;

  private List<Boolean> hasDataRemaining;

  // heap only need to store time
  private PriorityQueue<Long> timeHeap;

  private Set<Long> timeSet;

  public DataSetWithoutTimeGenerator(List<Path> paths, List<TSDataType> dataTypes, List<ISeriesReader> readers) throws IOException {
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
