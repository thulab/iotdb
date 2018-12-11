package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;

public class QueryDataSetForQueryWithQueryFilterImpl extends QueryDataSet {

  private TimestampGenerator timestampGenerator;
  private LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries;

  public QueryDataSetForQueryWithQueryFilterImpl(TimestampGenerator timestampGenerator,
                                                 LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries) {
    this.timestampGenerator = timestampGenerator;
    this.readersOfSelectedSeries = readersOfSelectedSeries;
  }

  @Override
  public boolean hasNext() throws IOException {
    return timestampGenerator.hasNext();
  }

  @Override
  public RowRecord next() throws IOException {
    long timestamp = timestampGenerator.next();
    RowRecord rowRecord = new RowRecord(timestamp);
    for (Path path : readersOfSelectedSeries.keySet()) {
      SeriesReaderByTimeStamp seriesReaderByTimestamp = readersOfSelectedSeries.get(path);
      rowRecord.putField(path, seriesReaderByTimestamp.getValueInTimestamp(timestamp));
    }
    return rowRecord;
  }
}
