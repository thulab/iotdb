package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;

public class LocalTombstoneFile extends TombstoneFile {

    public LocalTombstoneFile(String filePath) throws IOException {
        this.filePath = filePath;
        this.accessor = new LocalTombstoneAccessor(filePath);
    }
}
