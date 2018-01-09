package cn.edu.tsinghua.iotdb.newwritelog.writelognode;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.newwritelog.LogPosition;
import cn.edu.tsinghua.iotdb.newwritelog.RecoverStage;
import cn.edu.tsinghua.iotdb.newwritelog.replay.LogReplayer;
import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.utils.FileUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iotdb.newwritelog.RecoverStage.*;

/**
 * This WriteLogNode is used to manage write ahead logs of a single FileNode.
 */
public class ExclusiveWriteLogNode implements WriteLogNode {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);

    private static final String WAL_FILE_NAME = "wal";

    private static final String OLD_SUFFIX = "-old";

    private static final String RECOVER_FLAG_NAME = "recover-flag";

    private static final String RECOVER_SUFFIX = "-recover";

    /**
     * This should be the same as the corresponding FileNode's name.
     */
    private String identifier;

    private String logDirectory;

    private RecoverStage currStage;

    private RandomAccessFile currentFile;

    private LogReplayer replayer = new LogReplayer();

    private List<byte[]> logCache = new ArrayList<>();

    private String recoveryFlagPath;

    private String restoreFilePath;

    private String processorStoreFilePath;

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    public ExclusiveWriteLogNode(String identifier, String restoreFilePath, String processorStoreFilePath) throws IOException {
        this.identifier = identifier;
        this.logDirectory = config.walFolder + File.separator + this.identifier;
        new File(logDirectory).mkdirs();

        this.restoreFilePath = restoreFilePath;
        this.processorStoreFilePath = processorStoreFilePath;
        currStage = RecoverStage.init;
    }

    /*
    Return value is of no use in this implementation.
     */
    @Override
    public LogPosition write(PhysicalPlan plan) throws IOException {
        lockForWrite();
        byte[] logBytes = PhysicalPlanLogTransfer.operatorToLog(plan);
        logCache.add(logBytes);

        if(logCache.size() > config.flushWalThreshold) {
            sync();
        }
        unlockForWrite();
        return null;
    }

    @Override
    public void recover() throws RecoverException {
        currStage = determineStage();
        if(currStage != null)
            recoverAtStage(currStage);
    }

    @Override
    public void close() throws IOException {
        sync();
        try {
            this.currentFile.close();
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
        if(!oldLogFile.renameTo(new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX)))
            logger.error("Log node {} renaming log file failed!", identifier);
        else
            logger.info("Log node {} renamed log file", identifier);
        try {
            this.currentFile = new RandomAccessFile(this.logDirectory + File.separator + WAL_FILE_NAME,"rw");
        } catch (FileNotFoundException e) {
            logger.error("Log node {} cannot open new file", identifier);
        }
    }

    /*
    Warning : caller must have lock.
     */
    @Override
    public void notifyEndFlush(List<LogPosition> logPositions) {
        discard();
    }

    private void lockForWrite(){
        // meaningless in current implementation
    }

    private void lockForSync() {
        // TODO : ask for a lock method
        // FileNodeManager.getInstance().writeLock(identifier);
    }

    private void unlockForWrite() {
        // meaningless in current implementation
    }

    private void unlockForSync() {
        // TODO : ask for an unlock method
        // FileNodeManager.getInstance().writeUnlock(identifier);
    }

    private void sync() throws FileNotFoundException {
        lockForSync();
        if(this.currentFile == null) {
            try {
                this.currentFile = new RandomAccessFile(this.logDirectory + File.separator + WAL_FILE_NAME,"rw");
            } catch (FileNotFoundException e) {
                logger.error("Unable to create write log node : {}", e.getMessage());
                throw e;
            }
        }
        logger.info("Log node {} starts sync, {} logs to be synced", identifier, logCache.size());
        try {
            currentFile.seek(currentFile.length());
            int totalSize = 0;
            for(byte[] bytes : logCache) {
                totalSize += 4 + bytes.length;
            }
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            for(byte[] bytes : logCache) {
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            }
            currentFile.write(buffer.array());
        } catch (IOException e) {
            logger.error("Log node {} sync failed because {}.", identifier, e.getMessage());
        }
        logCache.clear();
        logger.info("log node {} ends flush.", identifier);
        unlockForSync();
    }

    private RecoverStage determineStage() {
        File logDir = new File(logDirectory);
        if(!logDir.exists()) {
            logger.error("Log node {} directory does not exist, recover failed", identifier);
            return null;
        }
        // search for the flag file
        File[] files = logDir.listFiles((dir, name) -> name.contains(RECOVER_FLAG_NAME));

        if(files == null || files.length == 0) {
            File[] logFiles = logDir.listFiles((dir, name) -> name.contains(WAL_FILE_NAME));
            // no flag is set, and there exists log file, start from beginning.
            if(logFiles != null && logFiles.length > 0)
                return RecoverStage.backup;
            // no flag is set, and there is no log file, do not recover.
            else
                return null;
        }

        File flagFile = files[0];
        String flagName = flagFile.getName();
        recoveryFlagPath = flagFile.getPath();
        // the flag name is like "recover-flag-{flagType}"
        String[] parts = flagName.split("-");
        if(parts.length != 3) {
            logger.error("Log node {} invalid recover flag name {}", identifier, flagName);
            return null;
        }
        String stageName = parts[2];
        // if a flag of stage X is found, that means X had finished, so start from next stage
        if(stageName.equals(RecoverStage.backup.name()))
            return recoverFile;
        else if(stageName.equals(replayLog.toString()))
            return RecoverStage.cleanup;
        else {
            logger.error("Log node {} invalid recover flag name {}", identifier, flagName);
            return null;
        }
    }

    private void recoverAtStage(RecoverStage stage) throws RecoverException {
        switch (stage) {
            case init:
            case backup:
                backup();
                break;
            case recoverFile:
                recoverFile();
                break;
            case replayLog:
                replayLog();
                break;
            case cleanup:
                cleanup();
                break;
            default:
                logger.error("Invalid stage {}", stage);
        }
    }

    private void discard() {
        File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX);
        if(!oldLogFile.exists()) {
            logger.error("Old log file of {} does not exist", identifier);
        } else {
            if(oldLogFile.delete())
                logger.error("Old log file of {} cannot be deleted", identifier);
            else
                logger.info("Log node {} cleaned old file", identifier);
        }
    }

    private void setFlag(RecoverStage stage) {
        if(recoveryFlagPath == null) {
            recoveryFlagPath = logDirectory + File.separator + RECOVER_FLAG_NAME + "-" + stage.name();
            try {
                File flagFile = new File(recoveryFlagPath);
                if(!flagFile.createNewFile())
                    logger.error("Log node {} cannot set flag at stage {}",identifier, stage.name());
            } catch (IOException e) {
                logger.error("Log node {} cannot set flag at stage {}",identifier, stage.name());
            }
        } else {
            File flagFile = new File(recoveryFlagPath);
            recoveryFlagPath = recoveryFlagPath.replace("-" + currStage.name(), "-" + stage.name());
            if(!flagFile.renameTo(new File(recoveryFlagPath)))
                logger.error("Log node {} cannot update flag at stage {}",identifier, stage.name());
        }
    }

    private void cleanFlag() throws RecoverException {
        if(recoveryFlagPath != null) {
            File flagFile = new File(recoveryFlagPath);
            if(!flagFile.delete()) {
                logger.error("Log node {} cannot clean flag ", identifier);
                throw new RecoverException("Cannot clean flag");
            }
        }
    }

    private void backup() throws RecoverException {
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        if(!recoverRestoreFile.exists()) {
            try {
                FileUtils.fileCopy(new File(restoreFilePath), recoverRestoreFile);
            } catch (Exception e) {
                logger.error("Log node {} cannot backup restore file", identifier);
                throw new RecoverException("Cannot backup restore file, recovery aborted.");
            }
        }

        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        if(!recoverProcessorStoreFile.exists()) {
            try {
                FileUtils.fileCopy(new File(processorStoreFilePath), recoverProcessorStoreFile);
            } catch (Exception e) {
                logger.error("Log node {} cannot backup processor file", identifier);
                throw new RecoverException("Cannot backup processor file, recovery aborted.");
            }
        }

        setFlag(backup);
        currStage = recoverFile;
        logger.info("Log node {} backup ended", identifier);
        recoverFile();
    }

    private void recoverFile() throws RecoverException {
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        try {
            FileUtils.fileCopy(recoverRestoreFile, new File(restoreFilePath));
        } catch (Exception e) {
            logger.error("Log node {} cannot recover restore file", identifier);
            throw new RecoverException("Cannot recover restore file, recovery aborted.");
        }

        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        try {
            FileUtils.fileCopy(recoverProcessorStoreFile, new File(processorStoreFilePath) );
        } catch (Exception e) {
            logger.error("Log node {} cannot recover processor file", identifier);
            throw new RecoverException("Cannot recover processor file, recovery aborted.");
        }

        // TODO : notify file node to use restore file
        currStage = replayLog;
        logger.info("Log node {} recover files ended", identifier);
        replayLog();
    }

    private void replayLog() throws RecoverException {
        // if old log file exists, replay it first.
        File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX);
        int failedCnt = 0;
        if(oldLogFile.exists()) {
            RandomAccessFile oldRaf = null;
            try {
                oldRaf = new RandomAccessFile(oldLogFile, "r");
                int bufferSize = 4 * 1024 * 1024;
                // do not new buffer inside a loop
                byte[] buffer = new byte[bufferSize];
                while(oldRaf.getFilePointer() < oldRaf.length()) {
                    int logSize = oldRaf.readInt();
                     if(logSize > bufferSize) {
                         bufferSize = logSize;
                         buffer = new byte[bufferSize];
                     }
                    oldRaf.read(buffer, 0, logSize);
                     PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(buffer);
                    try {
                        replayer.replay(plan);
                    } catch (ProcessorException e) {
                        failedCnt ++;
                        logger.error("Log node {}, {}", identifier, e.getMessage());
                    }
                }
            } catch (IOException e) {
                logger.error("Log node {} cannot read old log file, because {}", e.getMessage());
                throw new RecoverException("Cannot read old log file, recovery aborted.");
            } finally {
                if(oldRaf != null) {
                    try {
                        oldRaf.close();
                    } catch (IOException e) {
                        logger.error("Log node {}, old log file cannot be closed", identifier);
                    }
                }
            }
        }
        // then replay new log
        File newLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME);
        if(newLogFile.exists()) {
            RandomAccessFile newRaf = null;
            try {
                newRaf = new RandomAccessFile(newLogFile, "r");
                int bufferSize = 4 * 1024 * 1024;
                byte[] buffer = new byte[bufferSize];
                while(newRaf.getFilePointer() < newRaf.length()) {
                    int logSize = newRaf.readInt();
                    if(logSize > bufferSize) {
                        bufferSize = logSize;
                        buffer = new byte[bufferSize];
                    }
                    newRaf.read(buffer, 0, logSize);
                    PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(buffer);
                    try {
                        replayer.replay(plan);
                    } catch (ProcessorException e) {
                        failedCnt++;
                        logger.error("Log node {}, {}", identifier, e.getMessage());
                    }
                }
            } catch (IOException e) {
                logger.error("Log node {} cannot read old log file, because {}", e.getMessage());
                throw new RecoverException("Cannot read new log file, recovery aborted.");
            } finally {
                if(newRaf != null) {
                    try {
                        newRaf.close();
                    } catch (IOException e) {
                        logger.error("Log node {}, old log file cannot be closed", identifier);
                    }
                }
            }
        }
        // TODO : do we need to proceed if there are failed logs ?
        if(failedCnt > 0)
            throw new RecoverException("There are " + failedCnt + " logs failed to recover, see logs above for details");
        // TODO : ask FileNode to perform a synchronized flush
        currStage = cleanup;
        setFlag(replayLog);
        logger.info("Log node {} replay ended.", identifier);
        cleanup();
    }

    private void cleanup() throws RecoverException {
        // clean recovery files
        List<String> failedFiles = new ArrayList<>();
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        if(recoverRestoreFile.exists()) {
            if(!recoverRestoreFile.delete()){
                logger.error("Log node {} cannot delete backup restore file", identifier);
                failedFiles.add(recoverRestoreFilePath);
            }
        }
        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        if(recoverProcessorStoreFile.exists()) {
            if(!recoverProcessorStoreFile.delete()) {
                logger.error("Log node {} cannot delete backup processor store file", identifier);
                failedFiles.add(recoverProcessorStoreFilePath);
            }
        }
        // clean log file
        File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX);
        if(oldLogFile.exists()) {
            if(!oldLogFile.delete()) {
                logger.error("Log node {} cannot delete old log file", identifier);
                failedFiles.add(oldLogFile.getPath());
            }
        }
        File newLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME);
        if(newLogFile.exists()) {
            if(!newLogFile.delete()) {
                logger.error("Log node {} cannot delete new log file", identifier);
                failedFiles.add(newLogFile.getPath());
            }
        }
        if(failedFiles.size() > 0)
            throw new RecoverException("File clean failed. Failed files are " + failedFiles.toString());
        // clean flag
        currStage = init;
        cleanFlag();
        logger.info("Log node {} cleanup ended.", identifier);
    }

    public String toString() {
        return "Log node " + identifier;
    }
}
