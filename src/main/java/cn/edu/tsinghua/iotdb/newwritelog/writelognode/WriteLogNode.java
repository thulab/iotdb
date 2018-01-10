package cn.edu.tsinghua.iotdb.newwritelog.writelognode;

import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.newwritelog.LogPosition;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public interface WriteLogNode {

    /**
     * Write a log which implements LogSerializable.
     * First, the log will be conveyed to byte[] by codec. Then the byte[] will be put into a cache.
     * If necessary, the logs in the cache will be synced to disk.
     * @param plan
     * @return The position to be written of the log.
     */
    LogPosition write(PhysicalPlan plan) throws FileNotFoundException, IOException;

    /**
     * First judge the stage of recovery by status of files, and then recover from that stage.
     */
    void recover() throws RecoverException;

    /**
     * Sync and close streams.
     */
    void close() throws FileNotFoundException, IOException;

    /**
     * Write what in cache to disk.
     */
    void forceSync() throws FileNotFoundException, IOException;

    /**
     * When a FileNode attempts to start a flush, this method must be called to rename log file.
     */
    void notifyStartFlush() throws IOException;

    /**
     * When the flush of a FlieNode ends, this method must be called to check if log file needs cleaning.
     */
    void notifyEndFlush(List<LogPosition> logPositions);

    /**
     *
     * @return the identifier of this log node.
     */
    String getIdentifier();

    /**
     *
     * @return the directory where wal file is placed.
     */
    String getLogDirectory();
}
