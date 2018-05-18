package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;

/**
 * This class is used to switch different versions of tombstoneFile, for the need of future flexibility.
 */
public class TombstoneFileFactory {

    private static ITombstoneFileFactory inUseFactory = new LocalTombstoneFileFactory();

    public static ITombstoneFileFactory getFactory() {
        return inUseFactory;
    }

    private static class LocalTombstoneFileFactory implements ITombstoneFileFactory {

        @Override
        public TombstoneFile getTombstoneFile(String filePath) throws IOException {
            return new LocalTombstoneFile(filePath);
        }
    }
}
