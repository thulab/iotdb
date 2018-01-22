package cn.edu.tsinghua.iotdb.newwritelog.writelognode;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.newwritelog.IO.ILogWriter;
import cn.edu.tsinghua.iotdb.newwritelog.IO.RAFLogWriter;
import cn.edu.tsinghua.iotdb.newwritelog.LogPosition;
import cn.edu.tsinghua.iotdb.newwritelog.recover.ExclusiveLogRecoverPerformer;
import cn.edu.tsinghua.iotdb.newwritelog.recover.RecoverPerformer;
import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This WriteLogNode is used to manage write ahead logs of a single FileNode.
 */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);

    public static final String WAL_FILE_NAME = "wal";

    public static final String OLD_SUFFIX = "-old";

    /**
     * This should be the same as the corresponding FileNode's name.
     */
    private String identifier;

    private String logDirectory;

    private ILogWriter currentFileWriter;

    private RecoverPerformer recoverPerformer;

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    private List<byte[]> logCache = new ArrayList<>(config.flushWalThreshold);

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public ExclusiveWriteLogNode(String identifier, String restoreFilePath, String processorStoreFilePath) {
        this.identifier = identifier;
        this.logDirectory = config.walFolder + File.separator + this.identifier;
        new File(logDirectory).mkdirs();

        recoverPerformer = new ExclusiveLogRecoverPerformer(restoreFilePath, processorStoreFilePath, this);
        currentFileWriter = new RAFLogWriter(logDirectory + File.separator + WAL_FILE_NAME);
    }

    public void setRecoverPerformer(RecoverPerformer recoverPerformer) {
        this.recoverPerformer = recoverPerformer;
    }

    /*
    Return value is of no use in this implementation.
     */
    @Override
    public LogPosition write(PhysicalPlan plan) throws IOException {
        lockForWrite();
        try {
            byte[] logBytes = PhysicalPlanLogTransfer.operatorToLog(plan);
            logCache.add(logBytes);

            if (logCache.size() >= config.flushWalThreshold) {
                sync();
            }
        } finally {
            unlockForWrite();
        }
        return null;
    }

    @Override
    public void recover() throws RecoverException {
        recoverPerformer.recover();
    }

    @Override
    public void close() throws IOException {
        sync();
        try {
            this.currentFileWriter.close();
            logger.debug("Log node {} closed successfully", identifier);
        } catch (IOException e) {
            logger.error("Cannot close log node {} because {}", identifier, e.getMessage());
        }
    }

    @Override
    public void forceSync() throws IOException {
        sync();
    }

    /*
    Warning : caller must have lock.
     */
    @Override
    public void notifyStartFlush() throws IOException {
        close();
        File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME);
        if(!oldLogFile.exists())
            return;
        if(!oldLogFile.renameTo(new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX)))
            logger.error("Log node {} renaming log file failed!", identifier);
        else
            logger.debug("Log node {} renamed log file", identifier);
    }

    /*
    Warning : caller must have lock.
     */
    @Override
    public void notifyEndFlush(List<LogPosition> logPositions) {
        discard();
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getLogDirectory() {
        return logDirectory;
    }

    @Override
    public void delete() throws IOException {
        logCache.clear();
        if(currentFileWriter != null)
            currentFileWriter.close();
        FileUtils.recurrentDelete(new File(logDirectory));
    }

    private void lockForWrite(){
        lock.writeLock().lock();
    }

    // other means sync and delete
    private void lockForOther() {
       lock.writeLock().lock();
    }

    private void unlockForWrite() {
        lock.writeLock().unlock();
    }

    private void unlockForOther() {
       lock.writeLock().unlock();
    }

    private void sync() {
        lockForOther();
        try {
            logger.debug("Log node {} starts sync, {} logs to be synced", identifier, logCache.size());
            if(logCache.size() == 0) {
                return;
            }
            try {
                currentFileWriter.write(logCache);
            } catch (IOException e) {
                logger.error("Log node {} sync failed because {}.", identifier, e.getMessage());
            }
            logCache.clear();
            logger.debug("Log node {} ends sync.", identifier);
        } finally {
            unlockForOther();
        }
    }

    private void discard() {
        File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX);
        if(!oldLogFile.exists()) {
            logger.debug("No old log to be deleted");
        } else {
            if(!oldLogFile.delete())
                logger.error("Old log file of {} cannot be deleted", identifier);
            else
                logger.debug("Log node {} cleaned old file", identifier);
        }
    }

    public String toString() {
        return "Log node " + identifier;
    }

    public String getFileNodeName() {
        return identifier.split("-")[0];
    }

    @Override
    public int compareTo(ExclusiveWriteLogNode o) {
        return this.identifier.compareTo(o.identifier);
    }
}
