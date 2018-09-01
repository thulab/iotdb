package cn.edu.tsinghua.iotdb.query.management;

import java.io.IOException;
import java.util.HashMap;

import cn.edu.tsinghua.iotdb.query.reader.RecordReader;

/**
 * Used for read process, put the query structure in the cache for one query process.
 */
public class RecordReaderCacheManager {

    private HashMap<String, RecordReader> cache = new HashMap<>();

    public boolean containsRecordReader(String deltaObjectUID, String measurementID) {
        checkCacheInitialized();
        return cache.containsKey(getKey(deltaObjectUID, measurementID));
    }

    public RecordReader get(String deltaObjectUID, String measurementID) {
        checkCacheInitialized();
        return cache.get(getKey(deltaObjectUID, measurementID));
    }

    public void put(String deltaObjectUID, String measurementID, RecordReader recordReader) {
        checkCacheInitialized();
        cache.put(getKey(deltaObjectUID, measurementID), recordReader);
    }

    public RecordReader remove(String deltaObjectUID, String measurementID) {
        checkCacheInitialized();
        return cache.remove(getKey(deltaObjectUID, measurementID));
    }

    public void clear() throws IOException {
        for (RecordReader reader : cache.values()) {
            reader.closeFileStream();
            reader.closeFileStreamForOneRequest();
        }
        cache.clear();
    }

    private String getKey(String deltaObjectUID, String measurementID) {
        return Thread.currentThread().getId() + "#" + deltaObjectUID + "#" + measurementID;
    }

    private void checkCacheInitialized() {
        if (cache == null) {
            cache=new HashMap<>();
        }
    }
}
