package cn.edu.tsinghua.iotdb.newwritelog.recover;

import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.newwritelog.RecoverStage;
import cn.edu.tsinghua.iotdb.newwritelog.replay.ConcretLogReplayer;
import cn.edu.tsinghua.iotdb.newwritelog.replay.LogReplayer;
import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.utils.FileUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iotdb.newwritelog.RecoverStage.*;

public class ExclusiveLogRecoverPerformer implements RecoverPerformer {

    private static final Logger logger = LoggerFactory.getLogger(ExclusiveLogRecoverPerformer.class);

    public static final String RECOVER_FLAG_NAME = "recover-flag";

    public static final String RECOVER_SUFFIX = "-recover";

    private ExclusiveWriteLogNode writeLogNode;

    private String recoveryFlagPath;

    private String restoreFilePath;

    private String processorStoreFilePath;

    private RecoverStage currStage;

    private LogReplayer replayer = new ConcretLogReplayer();

    private RecoverPerformer fileNodeRecoverPerformer;

    public ExclusiveLogRecoverPerformer(String restoreFilePath, String processorStoreFilePath, ExclusiveWriteLogNode logNode) {
        this.restoreFilePath = restoreFilePath;
        this.processorStoreFilePath = processorStoreFilePath;
        this.writeLogNode = logNode;
        this.fileNodeRecoverPerformer = new FileNodeRecoverPerformer(writeLogNode.getIdentifier());
    }

    public void setFileNodeRecoverPerformer(RecoverPerformer fileNodeRecoverPerformer) {
        this.fileNodeRecoverPerformer = fileNodeRecoverPerformer;
    }

    public void setReplayer(LogReplayer replayer) {
        this.replayer = replayer;
    }

    @Override
    public void recover() throws RecoverException {
        try {
            writeLogNode.close();
        } catch (IOException e) {
            logger.error("Cannot close write log {} node before recover!", writeLogNode.getIdentifier());
            throw new RecoverException(String.format("Cannot close write log %s node before recover!", writeLogNode.getIdentifier()));
        }
        currStage = determineStage();
       if(currStage != null)
           recoverAtStage(currStage);
    }

    private RecoverStage determineStage() throws RecoverException {
        File logDir = new File(writeLogNode.getLogDirectory());
        if(!logDir.exists()) {
            logger.error("Log node {} directory does not exist, recover failed", writeLogNode.getLogDirectory());
            throw new RecoverException("No directory for log node " + writeLogNode.getIdentifier());
        }
        // search for the flag file
        File[] files = logDir.listFiles((dir, name) -> name.contains(RECOVER_FLAG_NAME));

        if(files == null || files.length == 0) {
            File[] logFiles = logDir.listFiles((dir, name) -> name.contains(ExclusiveWriteLogNode.WAL_FILE_NAME));
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
            logger.error("Log node {} invalid recover flag name {}", writeLogNode.getIdentifier(), flagName);
            throw new RecoverException("Illegal recover flag " + flagName);
        }
        String stageName = parts[2];
        // if a flag of stage X is found, that means X had finished, so start from next stage
        if(stageName.equals(RecoverStage.backup.name()))
            return recoverFile;
        else if(stageName.equals(replayLog.toString()))
            return RecoverStage.cleanup;
        else {
            logger.error("Log node {} invalid recover flag name {}", writeLogNode.getIdentifier(), flagName);
            throw new RecoverException("Illegal recover flag " + flagName);
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

    private void setFlag(RecoverStage stage) {
        if(recoveryFlagPath == null) {
            recoveryFlagPath = writeLogNode.getLogDirectory() + File.separator + RECOVER_FLAG_NAME + "-" + stage.name();
            try {
                File flagFile = new File(recoveryFlagPath);
                if(!flagFile.createNewFile())
                    logger.error("Log node {} cannot set flag at stage {}",writeLogNode.getLogDirectory(), stage.name());
            } catch (IOException e) {
                logger.error("Log node {} cannot set flag at stage {}",writeLogNode.getLogDirectory(), stage.name());
            }
        } else {
            File flagFile = new File(recoveryFlagPath);
            recoveryFlagPath = recoveryFlagPath.replace("-" + currStage.name(), "-" + stage.name());
            if(!flagFile.renameTo(new File(recoveryFlagPath)))
                logger.error("Log node {} cannot update flag at stage {}",writeLogNode.getLogDirectory(), stage.name());
        }
    }

    private void cleanFlag() throws RecoverException {
        if(recoveryFlagPath != null) {
            File flagFile = new File(recoveryFlagPath);
            if(!flagFile.delete()) {
                logger.error("Log node {} cannot clean flag ", writeLogNode.getLogDirectory());
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
                logger.error("Log node {} cannot backup restore file", writeLogNode.getLogDirectory());
                throw new RecoverException("Cannot backup restore file, recovery aborted.");
            }
        }

        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        if(!recoverProcessorStoreFile.exists()) {
            try {
                FileUtils.fileCopy(new File(processorStoreFilePath), recoverProcessorStoreFile);
            } catch (Exception e) {
                logger.error("Log node {} cannot backup processor file", writeLogNode.getLogDirectory());
                throw new RecoverException("Cannot backup processor file, recovery aborted.");
            }
        }

        setFlag(backup);
        currStage = recoverFile;
        logger.info("Log node {} backup ended", writeLogNode.getLogDirectory());
        recoverFile();
    }

    private void recoverFile() throws RecoverException {
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        try {
            FileUtils.fileCopy(recoverRestoreFile, new File(restoreFilePath));
        } catch (Exception e) {
            logger.error("Log node {} cannot recover restore file because {}", writeLogNode.getLogDirectory(), e.getMessage());
            throw new RecoverException("Cannot recover restore file, recovery aborted.");
        }

        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        try {
            FileUtils.fileCopy(recoverProcessorStoreFile, new File(processorStoreFilePath) );
        } catch (Exception e) {
            logger.error("Log node {} cannot recover processor file, because{}", writeLogNode.getLogDirectory(), e.getMessage());
            throw new RecoverException("Cannot recover processor file, recovery aborted.");
        }

        fileNodeRecoverPerformer.recover();

        currStage = replayLog;
        logger.info("Log node {} recover files ended", writeLogNode.getLogDirectory());
        replayLog();
    }

    private void replayLog() throws RecoverException {
        // if old log file exists, replay it first.
        File oldLogFile = new File(writeLogNode.getLogDirectory() + File.separator +
                ExclusiveWriteLogNode.WAL_FILE_NAME + ExclusiveWriteLogNode.OLD_SUFFIX);
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
                        logger.error("Log node {}, {}", writeLogNode.getLogDirectory(), e.getMessage());
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
                        logger.error("Log node {}, old log file cannot be closed", writeLogNode.getLogDirectory());
                    }
                }
            }
        }
        // then replay new log
        File newLogFile = new File(writeLogNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
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
                        logger.error("Log node {}, {}", writeLogNode.getLogDirectory(), e.getMessage());
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
                        logger.error("Log node {}, old log file cannot be closed", writeLogNode.getLogDirectory());
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
        logger.info("Log node {} replay ended.", writeLogNode.getLogDirectory());
        cleanup();
    }

    private void cleanup() throws RecoverException {
        // clean recovery files
        List<String> failedFiles = new ArrayList<>();
        String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
        File recoverRestoreFile = new File(recoverRestoreFilePath);
        if(recoverRestoreFile.exists()) {
            if(!recoverRestoreFile.delete()){
                logger.error("Log node {} cannot delete backup restore file", writeLogNode.getLogDirectory());
                failedFiles.add(recoverRestoreFilePath);
            }
        }
        String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
        File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
        if(recoverProcessorStoreFile.exists()) {
            if(!recoverProcessorStoreFile.delete()) {
                logger.error("Log node {} cannot delete backup processor store file", writeLogNode.getLogDirectory());
                failedFiles.add(recoverProcessorStoreFilePath);
            }
        }
        // clean log file
        File oldLogFile = new File(writeLogNode.getLogDirectory() + File.separator +
                ExclusiveWriteLogNode.WAL_FILE_NAME + ExclusiveWriteLogNode.OLD_SUFFIX);
        if(oldLogFile.exists()) {
            if(!oldLogFile.delete()) {
                logger.error("Log node {} cannot delete old log file", writeLogNode.getLogDirectory());
                failedFiles.add(oldLogFile.getPath());
            }
        }
        File newLogFile = new File(writeLogNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
        if(newLogFile.exists()) {
            if(!newLogFile.delete()) {
                logger.error("Log node {} cannot delete new log file", writeLogNode.getLogDirectory());
                failedFiles.add(newLogFile.getPath());
            }
        }
        if(failedFiles.size() > 0)
            throw new RecoverException("File clean failed. Failed files are " + failedFiles.toString());
        // clean flag
        currStage = init;
        cleanFlag();
        logger.info("Log node {} cleanup ended.", writeLogNode.getLogDirectory());
    }
}
