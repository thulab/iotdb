package cn.edu.tsinghua.iotdb.query.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to read <code>TsFileMetaData</code> and construct
 * file level reader which contains the information of <code>RowGroupReader</code>.
 *
 */
public class FileReader {
    private static final Logger logger = LoggerFactory.getLogger(FileReader.class);

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

    private TsFileMetaData fileMetaData;
    private ITsRandomAccessFileReader randomAccessFileReader; // file pointer

    private ArrayList<RowGroupReader> rowGroupReaderList;
    private Map<String, ArrayList<RowGroupReader>> rowGroupReaderMap;

    FileReader(ITsRandomAccessFileReader randomAccessFileReader) throws IOException {
        this.randomAccessFileReader = randomAccessFileReader;
        init();
    }

    /**
     * RowGroupMetaData is used to initialize the position of randomAccessFileReader.
     *
     * @param randomAccessFileReader file pointer
     * @param rowGroupMetaDataList RowGroupMetaDataList, no need to invoke init().
     */
    FileReader(ITsRandomAccessFileReader randomAccessFileReader, List<RowGroupMetaData> rowGroupMetaDataList) {
        this.randomAccessFileReader = randomAccessFileReader;
        initFromRowGroupMetadataList(rowGroupMetaDataList);
    }

    /**
     * FileReader initialize, constructing fileMetaData and rowGroupReaders.
     *
     * @throws IOException
     */
    private void init() throws IOException {
        long length = randomAccessFileReader.length();
        randomAccessFileReader.seek(length - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(length - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        this.fileMetaData = new TsFileMetaDataConverter()
                .toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(bais));

        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();
        //initFromRowGroupMetadataList(fileMetaData.getRowGroups());
    }

    private void initFromRowGroupMetadataList(List<RowGroupMetaData> rowGroupMetadataList) {
        rowGroupReaderList = new ArrayList<>();
        rowGroupReaderMap = new HashMap<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
            String key = rowGroupMetaData.getDeltaObjectID();
            RowGroupReader rowGroupReader = new RowGroupReader(rowGroupMetaData, randomAccessFileReader);
            rowGroupReaderList.add(rowGroupReader);
            if (!rowGroupReaderMap.containsKey(key)) {
                ArrayList<RowGroupReader> rowGroupReaderList = new ArrayList<>();
                rowGroupReaderList.add(rowGroupReader);
                rowGroupReaderMap.put(key, rowGroupReaderList);
            } else {
                rowGroupReaderMap.get(key).add(rowGroupReader);
            }
        }
    }

    public Map<String, ArrayList<RowGroupReader>> getRowGroupReaderMap() {
        return this.rowGroupReaderMap;
    }

    public ArrayList<RowGroupReader> getRowGroupReaderList() {
        return this.rowGroupReaderList;
    }

    /**
     * @param deltaObjectUID
     * @param index  from 0 to n-1
     * @return
     */
    public RowGroupReader getRowGroupReader(String deltaObjectUID, int index) {
        return this.rowGroupReaderMap.get(deltaObjectUID).get(index);
    }

    public TsFileMetaData getFileMetadata() {
        return this.fileMetaData;
    }

    public void close() throws IOException {
        this.randomAccessFileReader.close();
    }
}
