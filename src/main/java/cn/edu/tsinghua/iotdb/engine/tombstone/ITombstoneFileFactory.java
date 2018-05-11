package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;

/**
 * This interface is used to switch different versions of tombstoneFile, for the need of future flexibility.
 */
public interface ITombstoneFileFactory {
    TombstoneFile getTombstoneFile(String filePath) throws IOException;
}
