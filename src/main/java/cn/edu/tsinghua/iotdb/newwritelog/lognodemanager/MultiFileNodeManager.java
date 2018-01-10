package cn.edu.tsinghua.iotdb.newwritelog.lognodemanager;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class MultiFileNodeManager implements WriteLogNodeManager {

    private static final Logger logger = LoggerFactory.getLogger(MultiFileNodeManager.class);
    private Map<String, WriteLogNode> nodeMap;

    private Thread syncThread;
    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    private static class InstanceHolder {
        private static MultiFileNodeManager instance = new MultiFileNodeManager();
    }

    private MultiFileNodeManager() {
        nodeMap = new ConcurrentHashMap<>();
        syncThread = new Thread(() -> {
            while(true) {
                if(Thread.interrupted()){
                    logger.info("WAL sync thread exits.");
                    break;
                }

                for(WriteLogNode node : nodeMap.values()) {
                    try {
                        node.forceSync();
                    } catch (IOException e) {
                        logger.error("Cannot sync {}, because {}", node.toString(), e.toString());
                    }
                }

                try {
                    Thread.sleep(config.flushWalPeriodInMs);
                } catch (InterruptedException e) {
                    logger.info("WAL sync thread exits.");
                    break;
                }
            }
        }, "IoTDB-MultiFileNodeManager-Sync-Thread");
        syncThread.start();
    }

    static public MultiFileNodeManager getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    public WriteLogNode getNode(String identifier, String restoreFilePath, String processorStoreFilePath) throws IOException {
        WriteLogNode node = nodeMap.get(identifier);
        if(node == null) {
            node = new ExclusiveWriteLogNode(identifier, restoreFilePath, processorStoreFilePath);
            nodeMap.put(identifier, node);
        }
        return node;
    }

    /*
    Warning : caller must guarantee thread safety.
     */
    @Override
    public void recover() throws RecoverException {
        for(WriteLogNode node : nodeMap.values()) {
            try {
                node.recover();
            } catch (RecoverException e) {
                logger.error("{} failed to recover because {}", node.toString(), e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void close() {
        syncThread.interrupt();
        for(WriteLogNode node : nodeMap.values()) {
            try {
                node.close();
            } catch (IOException e) {
                logger.error("{} failed to close because {}", node.toString(), e.getMessage());
            }
        }
        nodeMap.clear();
    }
}
