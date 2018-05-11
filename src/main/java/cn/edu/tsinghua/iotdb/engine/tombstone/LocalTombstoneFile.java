package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;

/**
 * This class is an extension of TombstoneFile, with LocalTombstoneAccessor as its accessor.
 */
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
        synchronized (this) {
            // prevent multiple threads from creating different accessors
            if (this.accessor == null)
                return this.accessor = new LocalTombstoneAccessor(filePath);
            else
                return this.accessor;
        }
    }
}
