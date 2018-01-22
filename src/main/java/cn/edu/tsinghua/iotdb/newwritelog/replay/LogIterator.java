package cn.edu.tsinghua.iotdb.newwritelog.replay;

import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public class LogIterator implements Iterator<PhysicalPlan>{

    private static final Logger logger = LoggerFactory.getLogger(LogIterator.class);
    private RandomAccessFile logRAF;
    private String filepath;
    private int bufferSize = 4 * 1024 * 1024;
    private byte[] buffer = new byte[bufferSize];

    public LogIterator() {

    }

    public LogIterator(File logFile) throws FileNotFoundException {
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
            while(logRAF.getFilePointer() < logRAF.length()) {
                int logSize = logRAF.readInt();
                if(logSize > bufferSize) {
                    bufferSize = logSize;
                    buffer = new byte[bufferSize];
                }
                logRAF.read(buffer, 0, logSize);
                PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(buffer);
                return plan;
            }
        } catch (IOException e) {
            logger.error("Cannot read log file {}, because {}", filepath, e.getMessage());
        }
        return null;
    }

    public void close() {
        if (logRAF != null) {
            try {
                logRAF.close();
            } catch (IOException e) {
                logger.error("Cannot close log file {}", filepath);
            }
        }
    }

    public void open(File logFile) throws FileNotFoundException {
        logRAF = new RandomAccessFile(logFile, "r");
        this.filepath = logFile.getPath();
    }
}
