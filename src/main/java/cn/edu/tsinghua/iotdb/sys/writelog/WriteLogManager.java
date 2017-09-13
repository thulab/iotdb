package cn.edu.tsinghua.iotdb.sys.writelog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class WriteLogManager {
//    private static final Logger LOG = LoggerFactory.getLogger(WriteLogManager.class);
//    private static WriteLogManager instance = new WriteLogManager();

    // to determine whether system is in recovering process
    public static boolean isRecovering = false;
    // 0 represents BUFFERWRITE insert operation, 1 represents OVERFLOW insert operation
    public static final int BUFFERWRITER = 0, OVERFLOW = 1;

    private static class WriteLogManagerHolder {  
        private static final WriteLogManager INSTANCE = new WriteLogManager();  
    }
    private static ConcurrentHashMap<String, WriteLogNode> logNodeMaps;
    private static List<String> recoveryPathList = new ArrayList<>();


    private WriteLogManager() {
        if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
            logNodeMaps = new ConcurrentHashMap<>();
            // system log timing merge task
            ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
            long delay = 0;
            long interval = TsfileDBDescriptor.getInstance().getConfig().flushWalPeriodInMs;
            service.scheduleAtFixedRate(new LogMergeTimingTask(), delay, interval, TimeUnit.SECONDS);
        }
    }

    class LogMergeTimingTask implements Runnable {
        public void run() {
            try {
                for (Map.Entry<String, WriteLogNode> entry : logNodeMaps.entrySet()) {
                    entry.getValue().serializeMemoryToFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static final WriteLogManager getInstance() {
        return WriteLogManagerHolder.INSTANCE;
    }

    private static WriteLogNode getWriteLogNode(String fileNode) {
        if (logNodeMaps.containsKey(fileNode)) {
            return logNodeMaps.get(fileNode);
        }
        logNodeMaps.put(fileNode, new WriteLogNode(fileNode));
        return logNodeMaps.get(fileNode);
    }

    /**
     * Write the content of PhysicalPlan into WAL system.
     * Note that this method is only write UPDATE/DELETE operation.
     *
     * @param plan PhysicalPlan to serialize
     * @throws IOException  write WAL file error
     * @throws PathErrorException serialize <code>Path</code> error
     */
    public void write(PhysicalPlan plan) throws IOException, PathErrorException {
		List<Path> paths = plan.getPaths();
		MManager mManager = MManager.getInstance();
		Set<String> pathSet = new HashSet<>();
		for(Path p : paths){
			// already checked whether path exists at PhysicalGenerator
			pathSet.addAll(mManager.getPaths(p.getFullPath()));
		}
		for(String p : pathSet){
			getWriteLogNode(MManager.getInstance().getFileNameByPath(p)).write(plan);
		}
    }

    /**
     * Write the content of TSRecord into WAL system.
     * Note that this method is only write INSERT operation.
     *
     * @param record <code>TSRecord</code>
     * @param type determines the INSERT operation is Overflow or Bufferwrite
     * @throws IOException write WAL file error
     * @throws PathErrorException serialize <code>Path</code> error
     */
    public void write(TSRecord record, int type) throws IOException, PathErrorException {
        getWriteLogNode(MManager.getInstance().getFileNameByPath(record.deltaObjectId)).write(record, type);
    }

    public void startOverflowFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).overflowFlushStart();
    }

    public void endOverflowFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).overflowFlushEnd();
    }

    public void startBufferWriteFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).bufferFlushStart();
    }

    public void endBufferWriteFlush(String nsPath) throws IOException {
        getWriteLogNode(nsPath).bufferFlushEnd();
    }

    public void recovery() throws IOException {
        try {
            //TODO need optimize
            recoveryPathList = MManager.getInstance().getAllFileNames();
            Iterator<String> iterator = recoveryPathList.iterator();
            while (iterator.hasNext()) {
                String walPath = TsfileDBDescriptor.getInstance().getConfig().walFolder;
                if (walPath.length() > 0 && walPath.charAt(walPath.length() - 1) != File.separatorChar) {
                    walPath += File.separatorChar;
                }
                String filePath = walPath + iterator.next() + ".log";
                if (!new File(filePath).exists()) {
                    iterator.remove();
                }
            }
        } catch (PathErrorException e) {
            throw new IOException(e);
        }
    }

    public PhysicalPlan getPhysicalPlan() throws IOException {
        if (recoveryPathList.size() == 0)
            return null;

        Iterator<String> iterator = recoveryPathList.iterator();
        while (iterator.hasNext()) {
            WriteLogNode node = getWriteLogNode(iterator.next());
            node.recovery();
            PhysicalPlan plan = node.getPhysicalPlan();
            if (plan != null) {
                return plan;
            } else {
            	node.closeReadStream();
                iterator.remove();
            }
        }
        return null;
    }

    public void close() throws IOException {
        for (Map.Entry<String, WriteLogNode> entry : logNodeMaps.entrySet()) {
            entry.getValue().closeStreams();
        }
    }
}
