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
    private List<FileReader> fileReaderList;

    /** key: deltaObjectUID **/
    private Map<String, List<RowGroupReader>> rowGroupReaderMap = new HashMap<>();
    // private Map<String, List<RowGroupReader>> rowGroupReaderMap = new LRUMap<>(100000);

    /**
     *
     * @param rafList fileInputStreamList
     * @throws IOException
     */
    ReaderManager(List<ITsRandomAccessFileReader> rafList) throws IOException {
        this.rafList = rafList;
        this.fileReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();

        for (ITsRandomAccessFileReader taf : rafList) {
            FileReader reader = new FileReader(taf);
            fileReaderList.add(reader);
            // addRowGroupReadersToMap(reader);
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

        FileReader reader = new FileReader(unsealedFileReader, rowGroupMetadataList);
        fileReaderList.add(reader);
        // addRowGroupReadersToMap(reader);
    }

    List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) throws IOException {
        if (rowGroupReaderMap.containsKey(deltaObjectUID)) {
            return rowGroupReaderMap.get(deltaObjectUID);
        } else {
            List<RowGroupReader> rowGroupReaderList = new ArrayList<>();
            for (FileReader reader : fileReaderList) {
                if (reader.getFileMetaData() != null) {
                    if (reader.getFileMetaData().containsDeltaObject(deltaObjectUID)) {
                        rowGroupReaderList.addAll(reader.getRowGroupReaderListByDeltaObject(deltaObjectUID));
                    }
                } else {
                    if (reader.getRowGroupReaderMap().containsKey(deltaObjectUID)) {
                        rowGroupReaderList.addAll(reader.getRowGroupReaderMap().get(deltaObjectUID));
                    }
                }
            }
            rowGroupReaderMap.put(deltaObjectUID, rowGroupReaderList);
            return rowGroupReaderList;
        }
//        List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
//        if (ret == null) {
//            return new ArrayList<>();
//        }
//        return ret;
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
