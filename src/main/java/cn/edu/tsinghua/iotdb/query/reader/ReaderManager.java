package cn.edu.tsinghua.iotdb.query.reader;

import java.io.IOException;
import java.util.*;

import cn.edu.tsinghua.iotdb.engine.cache.RowGroupBlockMetaDataCache;
import cn.edu.tsinghua.iotdb.engine.cache.TsFileMetaDataCache;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import org.apache.derby.iapi.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adapter between <code>RecordReader</code> and <code>RowGroupReader</code> .
 */
public class ReaderManager {
    private static final Logger logger = LoggerFactory.getLogger(ReaderManager.class);

    /** file has been serialized, sealed **/
    private List<String> sealedFilePathList;

    /** unsealed file, at most one **/
    private String unSealedFilePath = null;

    /** RowGroupMetadata for unsealed file path **/
    private List<RowGroupMetaData> unSealedRowGroupMetadataList = null;

    /** key: deltaObjectUID **/
    private Map<String, List<RowGroupReader>> rowGroupReaderMap = new LinkedHashMap<>();

    /**
     *
     * @param sealedFilePathList fileInputStreamList
     */
    ReaderManager(List<String> sealedFilePathList) {
        this.sealedFilePathList = sealedFilePathList;
        //this.rowGroupReaderMap = new HashMap<>();
    }

    /**
     *
     * @param sealedFilePathList file node list
     * @param unsealedFilePath fileReader for unsealedFile
     * @param rowGroupMetadataList  RowGroupMetadata List for unsealedFile
     */
    ReaderManager(List<String> sealedFilePathList, String unsealedFilePath, List<RowGroupMetaData> rowGroupMetadataList) {
        this.sealedFilePathList = sealedFilePathList;
        this.unSealedFilePath = unsealedFilePath;
        this.unSealedRowGroupMetadataList = rowGroupMetadataList;
    }

    List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID, SingleSeriesFilterExpression timeFilter) throws IOException {
        if (rowGroupReaderMap.containsKey(deltaObjectUID)) {
            return rowGroupReaderMap.get(deltaObjectUID);
        } else {
            List<RowGroupReader> rowGroupReaderList = new ArrayList<>();

            // to examine whether sealed file has data
            for (String path : sealedFilePathList) {
                TsRandomAccessLocalFileReader fileReader = FileReaderMap.getInstance().get(path);
                TsFileMetaData tsFileMetaData = TsFileMetaDataCache.getInstance().get(path);
                if (tsFileMetaData.containsDeltaObject(deltaObjectUID)) {

                    // to filter some file whose (startTime, endTime) is not satisfied with the overflowTimeFilter
                    IntervalTimeVisitor metaFilter = new IntervalTimeVisitor();
                    if (timeFilter != null && !metaFilter.satisfy(timeFilter, tsFileMetaData.getDeltaObject(deltaObjectUID).startTime,
                            tsFileMetaData.getDeltaObject(deltaObjectUID).endTime))
                        continue;

                    TsRowGroupBlockMetaData tsRowGroupBlockMetaData = RowGroupBlockMetaDataCache.getInstance().get(path, deltaObjectUID, tsFileMetaData);
                    for (RowGroupMetaData meta : tsRowGroupBlockMetaData.getRowGroups()) {
                        //TODO parallelism could be used to speed up

                        RowGroupReader reader = new RowGroupReader(meta, fileReader);
                        rowGroupReaderList.add(reader);
                    }
                }
            }

            if (unSealedFilePath != null) {
                TsRandomAccessLocalFileReader fileReader = FileReaderMap.getInstance().get(unSealedFilePath);
                // TsRandomAccessLocalFileReader fileReader = new TsRandomAccessLocalFileReader(unSealedFilePath);
                for (RowGroupMetaData meta : unSealedRowGroupMetadataList) {
                    //TODO parallelism could be used to speed up

                    RowGroupReader reader = new RowGroupReader(meta, fileReader);
                    rowGroupReaderList.add(reader);
                }
            }

            rowGroupReaderMap.put(deltaObjectUID, rowGroupReaderList);
            return rowGroupReaderList;
        }
    }

    public void closeFileStream() {
        try {
            FileReaderMap.getInstance().close();
        } catch (IOException e) {
            logger.error("can not close file for one ReaderManager");
            e.printStackTrace();
        }
    }

    public void clearReaderMaps() {
        rowGroupReaderMap.clear();
    }
}
