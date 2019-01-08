package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.concurrent.IoTThreadFactory;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.UnClosedTsFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p> Manage all opened file streams, to ensure that each file will be opened at most once.
 */
public class FileReaderManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileReaderManager.class);

    /**
     * max file stream storage number, must be lower than 65535
     */
    private static int maxCacheFileSize = 30000;

    /**
     * key of fileReaderMap file path, value of fileReaderMap is its unique reader.
     */
    private ConcurrentHashMap<String, TsFileSequenceReader> fileReaderMap;

    /**
     * key of fileReaderMap file path, value of fileReaderMap is its reference count.
     */
    private ConcurrentHashMap<String, AtomicInteger> referenceMap;

    private static class OverflowFileStreamManagerHelper {
        public static FileReaderManager INSTANCE = new FileReaderManager();
    }

    public static FileReaderManager getInstance() {
        return OverflowFileStreamManagerHelper.INSTANCE;
    }

    /**
     * Get the file reader of given file path.
     */
    public synchronized TsFileSequenceReader get(String filePath, boolean isUnClosed) throws IOException {

        if (!fileReaderMap.containsKey(filePath)) {

            if (fileReaderMap.size() >= maxCacheFileSize) {
                LOGGER.warn("Query has opened {} files !", fileReaderMap.size());
            }

            TsFileSequenceReader tsFileReader = isUnClosed ? new UnClosedTsFileReader(filePath) : new TsFileSequenceReader(filePath);

            fileReaderMap.put(filePath, tsFileReader);
            return tsFileReader;
        }

        return fileReaderMap.get(filePath);
    }

    public void closeAndRemoveAllOpenedReaders() throws IOException {
        for (Map.Entry<String, TsFileSequenceReader> entry : fileReaderMap.entrySet()) {
            entry.getValue().close();
            referenceMap.remove(entry.getKey());
            fileReaderMap.remove(entry.getKey());
        }
    }

    public synchronized void increaseFileReference(String filePath) {
        if (!referenceMap.containsKey(filePath)) {
            referenceMap.put(filePath, new AtomicInteger());
            referenceMap.get(filePath).set(0);
        }
        referenceMap.get(filePath).getAndIncrement();
    }

    public void decreaseFileReference(String filePath) {
        referenceMap.get(filePath).getAndDecrement();
    }

    /**
     * This method is used for the end of merge process when the file need to be deleted.
     * TODO need to be invoked by FileNodeProcessor when merge process is done
     */
    public void removeCacheFileReaders(String filePath) throws IOException {
        if (fileReaderMap.containsKey(filePath)) {
            fileReaderMap.get(filePath).close();
            fileReaderMap.remove(filePath);
            referenceMap.remove(filePath);
        }
    }

    /**
     * This method is only used for unit test
     */
    public synchronized boolean contains(String filePath) {
        return fileReaderMap.containsKey(filePath);
    }

    private FileReaderManager() {
        fileReaderMap = new ConcurrentHashMap<>();
        referenceMap = new ConcurrentHashMap<>();

        clearUnUsedFilesInFixTime();
    }

    private void clearUnUsedFilesInFixTime() {
        long examinePeriod = TsfileDBDescriptor.getInstance().getConfig().cacheFileReaderClearPeriod;

        ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1,
                new IoTThreadFactory("opended-files-manager"));

        service.scheduleAtFixedRate(() -> {
            synchronized (FileReaderManager.class) {
                for (Map.Entry<String, TsFileSequenceReader> entry : fileReaderMap.entrySet()) {
                    TsFileSequenceReader reader = entry.getValue();
                    int reference = referenceMap.get(entry.getKey()).get();

                    if (reference == 0) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            LOGGER.error("Can not close TsFileSequenceReader {} !", reader.getFileName());
                        }
                        fileReaderMap.remove(entry.getKey());
                        referenceMap.remove(entry.getKey());
                    }
                }
            }
        },0, examinePeriod, TimeUnit.MILLISECONDS);
    }
}