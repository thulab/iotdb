package cn.edu.tsinghua.iotdb.newwritelog.IO;

import java.io.IOException;
import java.util.List;

public interface ILogWriter {

    void write(List<byte[]> logCache) throws IOException;

    void close() throws IOException;
}
