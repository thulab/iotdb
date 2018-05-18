package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a memory representation of a tombstone file.
 */
public abstract class TombstoneFile {

    public static final String TOMBSTONE_SUFFIX = ".tombstone";

    /**
     * The path of the file, not necessary of being absolute.
     */
    protected String filePath;
    /**
     * All tombstones maintained in this file.
     */
    protected Map<String, Map<String, List<Tombstone>>> tombstones;
    /**
     * Storage accessor for writing and reading.
     */
    protected ITombstoneAccessor accessor;
    /**
     * A lock that prevents a in-use tombstone from being deleted.
     */
    private ReentrantLock lock = new ReentrantLock();

    /**
     *
     * @return All tombstones in this file as a Map(deltaObjectId, Map(deltaObjectId, List(Tombstone))).
     * @throws IOException
     */
    public Map<String, Map<String, List<Tombstone>>> getTombstonesMap() throws IOException {
        if (this.tombstones != null && this.accessor != null) {
            return Collections.unmodifiableMap(this.tombstones);
        }
        return Collections.unmodifiableMap(this.tombstones = getAccessor().readAll());
    }

    /**
     *
     * @return All tombstones in this file as a List.
     * @throws IOException
     */
    public List<Tombstone> getTombstonesList() throws IOException {
        if (this.tombstones == null || this.accessor == null) {
            this.tombstones = getAccessor().readAll();
        }
        List<Tombstone> retList = new ArrayList<>();
        for (Map<String, List<Tombstone>> deltaObjTombstones : this.tombstones.values()) {
            for (List<Tombstone> seriesTombstones : deltaObjTombstones.values()) {
                retList.addAll(seriesTombstones);
            }
        }
        return retList;
    }

    /**
     * Close the storage access, i.e., the stream.
     * @throws IOException
     */
    public void close() throws IOException {
        if(accessor != null)
            accessor.close();
        accessor = null;
    }

    /**
     * Write a tombstone in this file. Do not care whether the same tombstone exists or not.
     * @param tombstone
     * @throws IOException
     */
    public void append(Tombstone tombstone) throws IOException {
        getAccessor().append(tombstone);
        if(tombstones == null) {
            getTombstonesMap();
        } else {
            Map<String, List<Tombstone>> deltaObjTombstones = tombstones.computeIfAbsent(tombstone.deltaObjectId, k -> new HashMap<>());
            List<Tombstone> seriesTombstones = deltaObjTombstones.computeIfAbsent(tombstone.measurementId, k -> new ArrayList<>());
            seriesTombstones.add(tombstone);
        }
    }

    /**
     *
     * @return Whether this file contains tombstones, i.e., size of tombstones is zero or not.
     * @throws IOException
     */
    public abstract boolean isEmpty() throws IOException;

    /**
     * Delete this tombstone file from the lower storage.
     * @return
     * @throws IOException
     */
    public boolean delete() throws IOException {
        boolean state = getAccessor().delete();
        accessor = null;
       return state;
    }

    /**
     * Lock the file so it cannot be deleted or changed.
     */
    public void lock() {
        lock.lock();
    }

    /**
     * Unlock the file so it can be deleted or changed.
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * Open an access to the lower storage and return it.
     * @return
     * @throws IOException
     */
    public abstract ITombstoneAccessor getAccessor() throws IOException;
}
