package cn.edu.tsinghua.iotdb.queryV2.reader.merge;

import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;

import java.io.IOException;

public interface EngineSeriesReaderByTimeStamp extends IReader {

  TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException;

}
