package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TombstoneFile {

    public static final String TOMBSTONE_SUFFIX = ".tombstone";

    protected String filePath;
    protected List<Tombstone> tombstones;
    protected ITombstoneAccessor accessor;
    private ReentrantLock lock = new ReentrantLock();

    public List<Tombstone> getTombstones() throws IOException {
        if (this.tombstones != null && this.accessor != null) {
            return this.tombstones;
        }
        return this.tombstones = getAccessor().readAll();
    }

    public void close() throws IOException {
        if(accessor != null)
            accessor.close();
        accessor = null;
    }

    public void append(Tombstone tombstone) throws IOException {
        getAccessor().append(tombstone);
        if(tombstones != null)
            getTombstones().add(tombstone);
    }

    public abstract boolean isEmpty() throws IOException;

    public boolean delete() throws IOException {
        boolean state = getAccessor().delete();
        accessor = null;
       return state;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public abstract ITombstoneAccessor getAccessor() throws IOException;
}
