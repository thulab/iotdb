package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;
import java.util.List;

public abstract class TombstoneFile {

    public static final String TOMBSTONE_SUFFIX = ".tombstone";

    protected String filePath;
    protected List<Tombstone> tombstones;
    protected ITombstoneAccessor accessor;

    public List<Tombstone> getTombstones() throws IOException {
        if (this.tombstones != null) {
            return this.tombstones;
        }
        return this.tombstones = this.accessor.readAll();
    }

    public void close() throws IOException {
        accessor.close();
    }

    public void append(Tombstone tombstone) throws IOException {
        accessor.append(tombstone);
        getTombstones().add(tombstone);
    }
}
