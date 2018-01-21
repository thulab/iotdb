package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class OverflowUpdateOperationReaderImpl implements OverflowUpdateOperationReader{
    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowUpdateOperationReaderImpl.class);
    private List<OverflowUpdateOperation> updataOperations;
    private int index = 0;
    private Filter<?> filter;

    public OverflowUpdateOperationReaderImpl(DynamicOneColumnData memoryUpdate, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList,
                                             TSDataType dataType) {
        for (int i = timeSeriesChunkMetaDataList.size() - 1; i >= 0; i--) {
            TimeSeriesChunkMetaData seriesMetaData = timeSeriesChunkMetaDataList.get(i);
            if (!seriesMetaData.getVInTimeSeriesChunkMetaData().getDataType().equals(dataType)) {
                continue;
            }
            int chunkSize = (int) seriesMetaData.getTotalByteSize();
            long offset = seriesMetaData.getProperties().getFileOffset();
//            InputStream in = overflowFileIO.getSeriesChunkBytes(chunkSize, offset);
//            try {
//                newerData = workingOverflowIndex.queryFileBlock(timeFilter, valueFilter, freqFilter, in, newerData);
//            } catch (IOException e) {
//                LOGGER.error("Read overflow file block failed, reason {}", e.getMessage());
//                // should throw the reason of the exception and handled by high
//                // level function
//            }
        }

    }

    @Override
    public boolean hasNext() {
        return index < updataOperations.size();
    }

    @Override
    public OverflowUpdateOperation next() {
        return updataOperations.get(index);
    }

    @Override
    public void close() throws IOException {

    }
}
