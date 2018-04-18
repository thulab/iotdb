package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;
import java.util.List;

public interface ITombstoneAccessor {

    List<Tombstone> readAll() throws IOException;

    void append(Tombstone tombstone) throws IOException;

    void append(List<Tombstone> tombstones) throws IOException;

    void append(String seriesName, long deleteTimestamp, long executeTimestamp) throws IOException;

    void close() throws IOException;
}
