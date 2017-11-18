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

/**
 * This class is used to construct FileReader. <br>
 * It is an adapter between <code>RecordReader</code> and <code>FileReader</code>.
 *
 */

public class ReaderManager {

    private List<ITsRandomAccessFileReader> rafList; // file has been serialized, sealed

    private Map<String, List<RowGroupReader>> rowGroupReaderMap;
    private List<RowGroupReader> rowGroupReaderList;

    /**
     *
     * @param rafList fileInputStreamList
     * @throws IOException
     */
    ReaderManager(List<ITsRandomAccessFileReader> rafList) throws IOException {
        this.rafList = rafList;
        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();

        for (ITsRandomAccessFileReader taf : rafList) {
            FileReader reader = new FileReader(taf);
            addRowGroupReadersToMap(reader);
            addRowGroupReadersToList(reader);
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
        addRowGroupReadersToMap(reader);
        addRowGroupReadersToList(reader);
    }

    private void addRowGroupReadersToMap(FileReader fileReader) {
        Map<String, List<RowGroupReader>> rgrMap = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rgrMap.keySet()) {
            if (rowGroupReaderMap.containsKey(deltaObjectUID)) {
                rowGroupReaderMap.get(deltaObjectUID).addAll(rgrMap.get(deltaObjectUID));
            } else {
                rowGroupReaderMap.put(deltaObjectUID, rgrMap.get(deltaObjectUID));
            }
        }
    }

    private void addRowGroupReadersToList(FileReader fileReader) {
        this.rowGroupReaderList.addAll(fileReader.getRowGroupReaderList());
    }

    List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) {
        List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjectUID);
        if (ret == null) {
            return new ArrayList<>();
        }
        return ret;
    }

    /**
     * @throws IOException
     */
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
