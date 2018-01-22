package cn.edu.tsinghua.iotdb.newwritelog.IO;

import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RAFLogReader implements ILogReader {

    private static final Logger logger = LoggerFactory.getLogger(RAFLogReader.class);
    private RandomAccessFile logRAF;
    private String filepath;
    private int bufferSize = 4 * 1024 * 1024;
    private byte[] buffer = new byte[bufferSize];

    public RAFLogReader() {

    }

    public RAFLogReader(File logFile) throws FileNotFoundException {
        open(logFile);
    }

    @Override
    public boolean hasNext() {
        try {
            return logRAF.getFilePointer() < logRAF.length();
        } catch (IOException e) {
            logger.error("Cannot read log file {}, because {}", filepath, e.getMessage());
        }
        return false;
    }

    @Override
    public PhysicalPlan next() {
        try {
            int logSize = logRAF.readInt();
            if (logSize > bufferSize) {
                bufferSize = logSize;
                buffer = new byte[bufferSize];
            }
            logRAF.read(buffer, 0, logSize);
            PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(buffer);
            return plan;
        } catch (IOException e) {
            logger.error("Cannot read log file {}, because {}", filepath, e.getMessage());
        }
        return null;
    }

    @Override
    public void close() {
        if (logRAF != null) {
            try {
                logRAF.close();
            } catch (IOException e) {
                logger.error("Cannot close log file {}", filepath);
            }
        }
    }

    @Override
    public void open(File logFile) throws FileNotFoundException {
        logRAF = new RandomAccessFile(logFile, "r");
        this.filepath = logFile.getPath();
    }
}
