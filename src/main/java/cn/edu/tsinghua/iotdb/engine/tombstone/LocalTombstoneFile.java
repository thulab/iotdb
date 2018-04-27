package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;

public class LocalTombstoneFile extends TombstoneFile {

    public LocalTombstoneFile(String filePath) throws IOException {
        this.filePath = filePath;
        this.accessor = new LocalTombstoneAccessor(filePath);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return getAccessor().isEmpty();
    }

    @Override
    public ITombstoneAccessor getAccessor() throws IOException {
        if(this.accessor != null)
            return this.accessor;
        return this.accessor = new LocalTombstoneAccessor(filePath);
    }
}
