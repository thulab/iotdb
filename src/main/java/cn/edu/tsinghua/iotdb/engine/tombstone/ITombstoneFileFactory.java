package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;

public interface ITombstoneFileFactory {
    TombstoneFile getTombstoneFile(String filePath) throws IOException;
}
