package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This interface provides write and read methods of tombstones from lower storage.
 */
public interface ITombstoneAccessor {

    /**
     * @return All tombstones in lower storage
     * @throws IOException
     */
    Map<String, Map<String, List<Tombstone>>> readAll() throws IOException;

    /**
     * Append a new tombstone to the lower storage.
     *
     * @param tombstone
     * @throws IOException
     */
    void append(Tombstone tombstone) throws IOException;

    /**
     * Append all given tombstones to the lower storage.
     *
     * @param tombstones
     * @throws IOException
     */
    void append(List<Tombstone> tombstones) throws IOException;

    /**
     * Create a tombstone with given params and append it to the lower storage.
     *
     * @param deltaObjectName
     * @param measurementName
     * @param deleteTimestamp
     * @param executeTimestamp
     * @throws IOException
     */
    void append(String deltaObjectName, String measurementName, long deleteTimestamp, long executeTimestamp) throws IOException;

    /**
     * Close file stream or other resources.
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * @return If lower storage contains any tombstone.
     * @throws IOException
     */
    boolean isEmpty() throws IOException;

    /**
     * Remove the tombstone file from lower storage.
     *
     * @return
     * @throws IOException
     */
    boolean delete() throws IOException;
}
