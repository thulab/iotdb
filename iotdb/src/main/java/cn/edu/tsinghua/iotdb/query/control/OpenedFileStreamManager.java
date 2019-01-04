package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.concurrent.IoTThreadFactory;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.UnClosedTsFileReader;
import cn.edu.tsinghua.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p> Manage all opened file streams, to ensure that each file will be opened at most once.
 */
public class OpenedFileStreamManager {

    /**
     * max file stream storage number, must be lower than 65535
     */
    private static int maxCacheFileSize = 30000;

    /**
     * expired time of cache file reader : 12 hours
     */
    private static long fileExpiredTime = 3600000 * 12;

    /**
     * expired period of cache file reader : 100 seconds
     */
    private static long examinePeriod = 100;

    /**
     * key of fileReaderMap file path, value of fileReaderMap is its unique reader and expired time.
     */
    private static ConcurrentHashMap<String, Pair<TsFileSequenceReader, Long>> fileReaderMap;

    private static class OverflowFileStreamManagerHelper {
        public static OpenedFileStreamManager INSTANCE = new OpenedFileStreamManager();
    }

    public static OpenedFileStreamManager getInstance() {
        return OverflowFileStreamManagerHelper.INSTANCE;
    }

    public static OpenedFileStreamManager getInstance(long fileExpiredTime, long examineTime) {
        OpenedFileStreamManager.fileExpiredTime = fileExpiredTime;
        OpenedFileStreamManager.examinePeriod = examineTime;
        clearExpiredFilesInFixTime();

        return OverflowFileStreamManagerHelper.INSTANCE;
    }

    private OpenedFileStreamManager() {
        fileReaderMap = new ConcurrentHashMap<>();
        
        clearExpiredFilesInFixTime();
    }
    
    private static void clearExpiredFilesInFixTime() {
        ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1,
                new IoTThreadFactory("opended-files-manager"));

        service.scheduleAtFixedRate(() -> {
            synchronized (OpenedFileStreamManager.class) {
                long currentTime = System.currentTimeMillis();

                for (Map.Entry<String, Pair<TsFileSequenceReader, Long>> entry : fileReaderMap.entrySet()) {
                    if (entry.getValue().right < currentTime) {
                        fileReaderMap.remove(entry.getKey());
                    }
                }
            }
        },0, examinePeriod, TimeUnit.SECONDS);
    }

    /**
     * Get the file reader of given file path.
     */
    public synchronized TsFileSequenceReader get(String filePath, boolean isUnSeqFile) throws IOException {

        long currentTime = System.currentTimeMillis();

        if (!fileReaderMap.containsKey(filePath) || fileReaderMap.get(filePath).right < currentTime) {

            if (fileReaderMap.size() >= maxCacheFileSize) {
                // TODO how to implement? remove oldest file or throw exception?
            }

            TsFileSequenceReader tsFileReader;
            if (isUnSeqFile) {
                tsFileReader = new UnClosedTsFileReader(filePath);
            } else {
                tsFileReader = new TsFileSequenceReader(filePath);
            }

            fileReaderMap.put(filePath, new Pair<>(tsFileReader, currentTime + fileExpiredTime));
            return tsFileReader;
        }

        return fileReaderMap.get(filePath).left;
    }

    public void closeAllOpenedFiles() throws IOException {
        for (Pair<TsFileSequenceReader, Long> pair : fileReaderMap.values()) {
            pair.left.close();
        }
        fileReaderMap.clear();
    }

    /**
     * This method is used for the end of merge process when the file need to be deleted.
     * TODO need to be invoked by FileNodeProcessor when merge process is done
     */
    public void removeCacheFileReaders(String filePath) throws IOException {
        if (fileReaderMap.containsKey(filePath)) {
            fileReaderMap.get(filePath).left.close();
            fileReaderMap.remove(filePath);
        }
    }

    /**
     * This method is only used for unit test
     */
    public synchronized boolean contains(String filePath) {
        return fileReaderMap.containsKey(filePath);
    }
}