package cn.edu.tsinghua.iotdb.newwritelog.lognodemanager;

import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.WriteLogNode;

import java.io.IOException;

/**
 * This interface provides accesses to WriteLogNode.
 */
public interface WriteLogNodeManager {

    /**
     * Get a WriteLogNode by a identifier like "{storageGroupName}-bufferwrite/overflow".
     * The WriteLogNode will be automatically created if not exist.
     * @param identifier
     * @param processorStoreFilePath
     * @param restoreFilePath
     * @return
     */
    WriteLogNode getNode(String identifier, String restoreFilePath, String processorStoreFilePath) throws IOException;

    /**
     * Make all node of this manager start recovery.
     */
    void recover() throws RecoverException;

    /**
     * Close all nodes.
     */
    void close();
}
