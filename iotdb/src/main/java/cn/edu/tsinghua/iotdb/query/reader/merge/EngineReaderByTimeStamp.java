package cn.edu.tsinghua.iotdb.query.reader.merge;

import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;

import java.io.IOException;

public interface EngineReaderByTimeStamp extends IReader {

  TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException;

}
