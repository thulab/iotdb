package cn.edu.tsinghua.iotdb.writelog.io;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.CRC32;

public class BufStreamLogReader implements ILogReader {

    private static final Logger logger = LoggerFactory.getLogger(BufStreamLogReader.class);

    private DataInputStream dataInputStream;
    private PhysicalPlan planBuffer;
    private File logFile;
    private int bufferSize = 4 * 1024 * 1024;
    private byte[] buffer = new byte[bufferSize];
    private CRC32 checkSummer = new CRC32();

    @Override
    public void open(File file) throws FileNotFoundException {
        dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        logFile = file;
    }

    @Override
    public void close() throws IOException {
        dataInputStream.close();
    }

    @Override
    public boolean hasNext() {
        if (planBuffer != null)
            return true;
        try {
            if (dataInputStream.available() < 12) {
                return false;
            }
        } catch (IOException e) {
            logger.error("Cannot read from log file {}, because {}", logFile.getPath(), e.getMessage());
            return false;
        }
        try {
            int logSize = dataInputStream.readInt();
            if (logSize > bufferSize) {
                bufferSize = logSize;
                buffer = new byte[bufferSize];
            }
            long checkSum = dataInputStream.readLong();
            dataInputStream.read(buffer, 0, logSize);
            checkSummer.reset();
            checkSummer.update(buffer, 0, logSize);
            if (checkSummer.getValue() != checkSum)
                return false;
            PhysicalPlan plan = PhysicalPlanLogTransfer.logToOperator(buffer);
            planBuffer = plan;
            return true;
        } catch (IOException e) {
            logger.error("Cannot read log file {}, because {}", logFile.getPath(), e.getMessage());
            return false;
        }
    }

    @Override
    public PhysicalPlan next() {
        PhysicalPlan ret = planBuffer;
        planBuffer = null;
        return ret;
    }
}
