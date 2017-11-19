package cn.edu.tsinghua.iotdb.query.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import org.apache.commons.collections4.map.LRUMap;

/**
 * This class is used to construct DbFileReader. <br>
 * It is an adapter between <code>RecordReader</code> and <code>DbFileReader</code>.
 *
 */
public class ReaderManager {

    /** file has been serialized, sealed **/
    private List<ITsRandomAccessFileReader> rafList;

    /** key: deltaObjectUID **/
    private Map<String, List<DbRowGroupReader>> rowGroupReaderMap;
    private LRUMap<String, List<DbRowGroupReader>> lruMap = new LRUMap<>(10000);

    /**
     *
     * @param rafList fileInputStreamList
     * @throws IOException
     */
    ReaderManager(List<ITsRandomAccessFileReader> rafList) throws IOException {
        this.rafList = rafList;
        rowGroupReaderMap = new HashMap<>();

        for (ITsRandomAccessFileReader taf : rafList) {
            DbFileReader reader = new DbFileReader(taf);
            addRowGroupReadersToMap(reader);
        }
    }

    /**
     *
     * @param rafList               file node list
     * @param unsealedFileReader fileReader for unsealedFile
     * @param rowGroupMetadataList  RowGroupMetadata List for unsealedFile
     * @throws IOException
     */
    ReaderManager(List<ITsRandomAccessFileReader> rafList,
                  ITsRandomAccessFileReader unsealedFileReader, List<RowGroupMetaData> rowGroupMetadataList) throws IOException {
        this(rafList);
        this.rafList.add(unsealedFileReader);

        DbFileReader reader = new DbFileReader(unsealedFileReader, rowGroupMetadataList);
        addRowGroupReadersToMap(reader);
    }

    private void addRowGroupReadersToMap(DbFileReader fileReader) {
        Map<String, List<DbRowGroupReader>> rowGroupReaderMap = fileReader.getDbRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaderMap.keySet()) {
            if (this.rowGroupReaderMap.containsKey(deltaObjectUID)) {
                this.rowGroupReaderMap.get(deltaObjectUID).addAll(rowGroupReaderMap.get(deltaObjectUID));
            } else {
                this.rowGroupReaderMap.put(deltaObjectUID, rowGroupReaderMap.get(deltaObjectUID));
            }
        }
    }

    List<DbRowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) {
        List<DbRowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
        if (ret == null) {
            return new ArrayList<>();
        }
        return ret;
    }

    public void close() throws IOException {
        for (ITsRandomAccessFileReader raf : rafList) {
            if (raf instanceof TsRandomAccessLocalFileReader) {
                raf.close();
            } else {
                raf.close();
            }
        }
    }
}
