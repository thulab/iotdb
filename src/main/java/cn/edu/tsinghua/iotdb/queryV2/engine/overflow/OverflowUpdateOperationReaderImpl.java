package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.iotdb.engine.overflow.treeV2.IntervalTreeOperation;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.List;

public class OverflowUpdateOperationReaderImpl implements OverflowUpdateOperationReader{
    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowUpdateOperationReaderImpl.class);
    //private List<OverflowUpdateOperation> updateOperations;
    private int index = 0;
    private Filter<?> filter;
    private DynamicOneColumnData updateOperations;
    private TSDataType dataType;

    public OverflowUpdateOperationReaderImpl(DynamicOneColumnData memoryUpdate,
                                             String path, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList,
                                             String mergePath, List<TimeSeriesChunkMetaData> mergeTimeSeriesChunkMetaDataList,
                                             SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                             TSDataType dataType) {
        this.dataType = dataType;
        IntervalTreeOperation overflowIndex = new IntervalTreeOperation(dataType);

        InputStream in = null;
        try {
            for (int i = timeSeriesChunkMetaDataList.size() - 1; i >= 0; i--) {
                TimeSeriesChunkMetaData seriesMetaData = timeSeriesChunkMetaDataList.get(i);
                if (!seriesMetaData.getVInTimeSeriesChunkMetaData().getDataType().equals(dataType)) {
                    continue;
                }
                int chunkSize = (int) seriesMetaData.getTotalByteSize();
                long offset = seriesMetaData.getProperties().getFileOffset();
                in = getSeriesChunkBytes(path, chunkSize, offset);
                updateOperations = overflowIndex.queryFileBlock(timeFilter, valueFilter, in, memoryUpdate);
            }

            for (int i = mergeTimeSeriesChunkMetaDataList.size() - 1; i >= 0; i--) {
                TimeSeriesChunkMetaData seriesMetaData = timeSeriesChunkMetaDataList.get(i);
                if (!seriesMetaData.getVInTimeSeriesChunkMetaData().getDataType().equals(dataType)) {
                    continue;
                }
                int chunkSize = (int) seriesMetaData.getTotalByteSize();
                long offset = seriesMetaData.getProperties().getFileOffset();
                in = getSeriesChunkBytes(mergePath, chunkSize, offset);
                updateOperations = overflowIndex.queryFileBlock(timeFilter, valueFilter, in, memoryUpdate);

            }
        }  catch (IOException e) {
            LOGGER.error("Read overflow file block failed, reason {}", e.getMessage());
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOGGER.error("Close overflow file stream failed, reason {}", e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    public InputStream getSeriesChunkBytes(String path, int chunkSize, long offset) {
        try {
            RandomAccessFile raf = new RandomAccessFile(path, "r");
            raf.seek(offset);
            byte[] chunk = new byte[chunkSize];
            int off = 0;
            int len = chunkSize;
            while (len > 0) {
                int num = raf.read(chunk, off, len);
                off = off + num;
                len = len - num;
            }
            return new ByteArrayInputStream(chunk);
        } catch (IOException e) {
            LOGGER.error("Read series chunk failed, reason is {}", e.getMessage());
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    @Override
    public boolean hasNext() {
        return index < updateOperations.valueLength;
    }

    @Override
    public OverflowUpdateOperation next() {
        switch (dataType) {
            case INT32:
                return new OverflowUpdateOperation(updateOperations.getTime(index * 2), updateOperations.getTime(index * 2 + 1),
                        new TsPrimitiveType.TsInt(updateOperations.getInt(index)));
            case INT64:
                return new OverflowUpdateOperation(updateOperations.getTime(index * 2), updateOperations.getTime(index * 2 + 1),
                        new TsPrimitiveType.TsLong(updateOperations.getLong(index)));
            case FLOAT:
                return new OverflowUpdateOperation(updateOperations.getTime(index * 2), updateOperations.getTime(index * 2 + 1),
                        new TsPrimitiveType.TsFloat(updateOperations.getFloat(index)));
            case DOUBLE:
                return new OverflowUpdateOperation(updateOperations.getTime(index * 2), updateOperations.getTime(index * 2 + 1),
                        new TsPrimitiveType.TsDouble(updateOperations.getDouble(index)));
            case TEXT:
                return new OverflowUpdateOperation(updateOperations.getTime(index * 2), updateOperations.getTime(index * 2 + 1),
                        new TsPrimitiveType.TsBinary(updateOperations.getBinary(index)));
            case BOOLEAN:
                return new OverflowUpdateOperation(updateOperations.getTime(index * 2), updateOperations.getTime(index * 2 + 1),
                        new TsPrimitiveType.TsBoolean(updateOperations.getBoolean(index)));
            default:
                LOGGER.error("unsupport overflow operation datatype");
                return null;
        }
    }

    @Override
    public void close() throws IOException {

    }
}
