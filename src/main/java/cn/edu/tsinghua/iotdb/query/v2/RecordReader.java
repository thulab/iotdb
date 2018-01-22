package cn.edu.tsinghua.iotdb.query.v2;

import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.v2.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.reader.ReaderManager;
import cn.edu.tsinghua.iotdb.query.reader.UpdateOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.List;

import static cn.edu.tsinghua.iotdb.query.engine.EngineUtils.copy;
import static cn.edu.tsinghua.iotdb.query.reader.ReaderUtils.getSingleValueVisitorByDataType;
import static cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory.and;

/**
 * <p>
 * A <code>RecordReader</code> contains all the data variables which is needed in read process.
 * Note that : it only contains the data of a (deltaObjectId, measurementId).
 * </p>
 */
public class RecordReader {

    static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    protected String deltaObjectId, measurementId;

    /** data type **/
    protected TSDataType dataType;

    /** compression type in this series **/
    public CompressionTypeName compressionTypeName;

    /** TsFile ReaderManager for current (deltaObjectId, measurementId) **/
    protected ReaderManager tsFileReaderManager;

    /** memtable data in memory **/
    protected RawSeriesChunk memRawSeriesChunk;

    /** overflow insert data reader **/
    protected SeriesReader overflowSeriesInsertReader;

    /** overflow update data reader **/
    protected OverflowOperationReader overflowOperationReader;

    /** series time filter, this filter is the filter **/
    protected SingleSeriesFilterExpression queryTimeFilter;
    protected SingleValueVisitor<?> singleTimeVisitor;

    /** series value filter **/
    protected SingleSeriesFilterExpression queryValueFilter;
    protected SingleValueVisitor<?> singleValueVisitor;

    /** memRawSeriesChunk + overflowSeriesInsertReader + overflowOperationReader **/
    protected InsertDynamicData insertMemoryData;


    public RecordReader(List<String> filePathList, String deltaObjectId, String measurementId, SingleSeriesFilterExpression queryTimeFilter,
                        SingleSeriesFilterExpression queryValueFilter, CompressionTypeName compressionTypeName)
            throws PathErrorException {
        this.tsFileReaderManager = new ReaderManager(filePathList);
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
        this.queryTimeFilter = queryTimeFilter;
        if (queryTimeFilter != null) {
            singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, queryTimeFilter);
        }
        this.queryValueFilter = queryValueFilter;
        if (queryValueFilter != null) {
            singleValueVisitor = getSingleValueVisitorByDataType(dataType, queryValueFilter);
        }
        this.compressionTypeName = compressionTypeName;
        this.dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
    }

    public RecordReader(List<String> filePathList, String unsealedFilePath,
                        List<RowGroupMetaData> rowGroupMetadataList, String deltaObjectId, String measurementId, CompressionTypeName compressionTypeName)
            throws PathErrorException {
        this.tsFileReaderManager = new ReaderManager(filePathList, unsealedFilePath, rowGroupMetadataList);
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
        this.compressionTypeName = compressionTypeName;
        this.dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
    }

    public void buildInsertMemoryData(SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter) {
    }

    public void closeFileStream() {
        tsFileReaderManager.closeFileStream();
    }

    public void clearReaderMaps() {
        tsFileReaderManager.clearReaderMaps();
    }
}
