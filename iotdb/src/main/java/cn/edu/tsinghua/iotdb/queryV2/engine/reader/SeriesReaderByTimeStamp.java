package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;

import java.io.IOException;

public interface SeriesReaderByTimeStamp extends ISeriesReader {

  TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException;

}
