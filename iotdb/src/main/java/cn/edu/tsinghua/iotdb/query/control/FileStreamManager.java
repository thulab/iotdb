package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p> Manage all opened file streams in a single IoTDB request.
 */
public class FileStreamManager {

    /**
     * key of fileStreamStore is job id, value of fileStreamStore is a map of filepath and its filereader.
     */
    private ConcurrentHashMap<Long, Map<String, TsFileSequenceReader>> fileStreamStore;

    private ConcurrentHashMap<String, MappedByteBuffer> mmapMemoryStreamStore = new ConcurrentHashMap<>();

    private AtomicInteger mappedByteBufferUsage = new AtomicInteger();

    private static class OverflowFileStreamManagerHelper {
        public static FileStreamManager INSTANCE = new FileStreamManager();
    }

    public static FileStreamManager getInstance() {
        return OverflowFileStreamManagerHelper.INSTANCE;
    }

    private FileStreamManager() {
        fileStreamStore = new ConcurrentHashMap<>();
    }

    /**
     * TODO maybe synchronized can be removed
     */
    public synchronized void put(Long jobId, String filePath, TsFileSequenceReader fileReader) {
        if (!fileStreamStore.containsKey(jobId)) {
            fileStreamStore.put(jobId, new ConcurrentHashMap<>());
        }

        if (!fileStreamStore.get(jobId).containsKey(filePath)) {
            fileStreamStore.get(jobId).put(filePath, fileReader);
        }
    }

    public synchronized boolean containsCachedReader(Long jobId, String filePath) {
        return fileStreamStore.containsKey(jobId) && fileStreamStore.get(jobId).containsKey(filePath);
    }

    public synchronized TsFileSequenceReader get(Long jobId, String filePath) {
        return fileStreamStore.get(jobId).get(filePath);
    }

    public synchronized void closeAll(Long jobId) throws IOException {
        if (fileStreamStore.containsKey(jobId)) {
            for (TsFileSequenceReader reader : fileStreamStore.get(jobId).values()) {
                reader.close();
            }
            fileStreamStore.remove(jobId);
        }
    }

    /**
     * Using MMap to replace RandomAccessFile
     */
    public synchronized MappedByteBuffer get(String path) throws IOException {
        if (!mmapMemoryStreamStore.containsKey(path)) {
            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
            MappedByteBuffer mappedByteBuffer =
                    randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length());
            mmapMemoryStreamStore.put(path, mappedByteBuffer);
            mappedByteBufferUsage.set(mappedByteBufferUsage.get() + (int) randomAccessFile.length());
        }
        return mmapMemoryStreamStore.get(path);
    }

    /**
     * Remove the MMap usage of given seriesPath.
     */
    public synchronized void removeMappedByteBuffer(String path) {
        if (mmapMemoryStreamStore.containsKey(path)) {
            MappedByteBuffer buffer = mmapMemoryStreamStore.get(path);
            mappedByteBufferUsage.set(mappedByteBufferUsage.get() - buffer.limit());
            ((DirectBuffer) buffer).cleaner().clean();
        }
    }

    public boolean containsMMap(String path) {
        return mmapMemoryStreamStore.containsKey(path);
    }

    public AtomicInteger getMappedByteBufferUsage() {
        return this.mappedByteBufferUsage;
    }
}