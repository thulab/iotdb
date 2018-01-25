package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.copy;
import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.and;

/**
 * A <code>RecordReader</code> contains all the data variables which is needed in read process.
 * Note that : it only contains the data of a (deltaObjectId, measurementId).
 *
 */
public class RecordReader {

    protected String deltaObjectId, measurementId;

    /** data type **/
    protected TSDataType dataType;

    /** compression type in this series **/
    public CompressionTypeName compressionTypeName;

    /** 1. TsFile ReaderManager for current (deltaObjectId, measurementId) **/
    public ReaderManager tsFileReaderManager;

    /** 2. bufferwrite data, the unsealed page **/
    public List<ByteArrayInputStream> bufferWritePageList;

    /** 3. bufferwrite insert data, the last page data in memory **/
    public DynamicOneColumnData lastPageInMemory;

    /** 4. overflow insert data **/
    public DynamicOneColumnData overflowInsertData;

    /** 5. overflow update data **/
    public DynamicOneColumnData overflowUpdate;
    protected UpdateOperation overflowUpdateOperation;

    /** 6. series time filter, this filter is the filter **/
    public SingleSeriesFilterExpression overflowTimeFilter;

    /** 7. series value filter **/
    public SingleSeriesFilterExpression valueFilter;

    /** bufferWritePageList + lastPageInMemory + overflow **/
    public InsertDynamicData insertMemoryData;

    // ====================================================================
    // unseqTsfile implementation


    /**
     * @param filePathList bufferwrite file has been serialized completely
     */
    public RecordReader(List<String> filePathList, String deltaObjectId, String measurementId,
                        DynamicOneColumnData lastPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws PathErrorException {
        this.tsFileReaderManager = new ReaderManager(filePathList);
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
        this.lastPageInMemory = lastPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        // to make sure that overflow data will not be null
        this.overflowInsertData = overflowInfo.get(0) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(0);
        this.overflowUpdate = overflowInfo.get(1) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(1);
        this.overflowTimeFilter = (SingleSeriesFilterExpression) overflowInfo.get(2);
        this.overflowUpdateOperation = new UpdateOperation(dataType, overflowUpdate);
    }

    /**
     * @param filePathList       bufferwrite file has been serialized completely
     * @param unsealedFilePath   unsealed file reader
     * @param rowGroupMetadataList unsealed RowGroupMetadataList to construct unsealedFileReader
     */
    public RecordReader(List<String> filePathList, String unsealedFilePath,
                        List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectId, String measurementId,
                        DynamicOneColumnData lastPageInMemory, List<ByteArrayInputStream> bufferWritePageList, CompressionTypeName compressionTypeName,
                        List<Object> overflowInfo) throws PathErrorException {
        this.tsFileReaderManager = new ReaderManager(filePathList, unsealedFilePath, rowGroupMetadataList);
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
        this.lastPageInMemory = lastPageInMemory;
        this.bufferWritePageList = bufferWritePageList;
        this.compressionTypeName = compressionTypeName;
        this.dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);

        // to make sure that overflow data will not be null
        this.overflowInsertData = overflowInfo.get(0) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(0);
        this.overflowUpdate = overflowInfo.get(1) == null ? new DynamicOneColumnData(dataType, true) : (DynamicOneColumnData) overflowInfo.get(1);
        this.overflowTimeFilter = (SingleSeriesFilterExpression) overflowInfo.get(2);
        this.overflowUpdateOperation = new UpdateOperation(dataType, overflowUpdate);
    }

    public void buildInsertMemoryData(SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter) {

        DynamicOneColumnData overflowUpdateCopy = copy(overflowUpdate);

        insertMemoryData = new InsertDynamicData(dataType, compressionTypeName,
                bufferWritePageList, lastPageInMemory,
                overflowInsertData, overflowUpdateCopy,
                mergeTimeFilter(overflowTimeFilter, queryTimeFilter), queryValueFilter);
    }

    protected SingleSeriesFilterExpression mergeTimeFilter(SingleSeriesFilterExpression overflowTimeFilter, SingleSeriesFilterExpression queryTimeFilter) {

        if (overflowTimeFilter == null && queryTimeFilter == null) {
            return null;
        } else if (overflowTimeFilter != null && queryTimeFilter == null) {
            return overflowTimeFilter;
        } else if (overflowTimeFilter == null) {
            return queryTimeFilter;
        } else {
            return (SingleSeriesFilterExpression) and(overflowTimeFilter, queryTimeFilter);
        }
    }

    public void closeFileStream() {
        tsFileReaderManager.closeFileStream();
    }

    public void clearReaderMaps() {
        tsFileReaderManager.clearReaderMaps();
    }

    // ====================================================================
    // unseqTsfile implementation
}
