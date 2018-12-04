package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.DynamicOneColumnData;

import java.io.IOException;
import java.util.List;

/**
 * <p> Series reader is used to query one series of one tsfile,
 * using this reader to query the value of a series with given timestamps.
 */
public class SeriesReaderByTimestamp extends SeriesReader {

    private long currentTimestamp;
    private boolean hasCacheLastTimeValuePair;
    private TimeValuePair cachedTimeValuePair;
    private int nextSeriesChunkIndex;

    private DynamicOneColumnData data = null;

    public SeriesReaderByTimestamp(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(chunkLoader, chunkMetaDataList);
        nextSeriesChunkIndex = 0;
        currentTimestamp = Long.MIN_VALUE;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCacheLastTimeValuePair && cachedTimeValuePair.getTimestamp() >= currentTimestamp) {
            return true;
        }
        if (chunkReaderInitialized) {
            ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp);
            if (chunkReader.hasNext()) {
                return true;
            }
        }
        while (nextSeriesChunkIndex < chunkMetaDataList.size()) {
            if (!chunkReaderInitialized) {
                ChunkMetaData chunkMetaData = chunkMetaDataList.get(nextSeriesChunkIndex);
                //maxTime >= currentTime
                if (chunkSatisfied(chunkMetaData)) {
                    initSeriesChunkReader(chunkMetaData);
                    ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp);
                    chunkReaderInitialized = true;
                    nextSeriesChunkIndex++;
                } else {
                    long minTimestamp = chunkMetaData.getStartTime();
                    long maxTimestamp = chunkMetaData.getEndTime();
                    if (maxTimestamp < currentTimestamp) {
                        continue;
                    } else if (minTimestamp > currentTimestamp) {
                        return false;
                    }
                }
            }
            if (chunkReader.hasNext()) {
                return true;
            } else {
                chunkReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasCacheLastTimeValuePair) {
            hasCacheLastTimeValuePair = false;
            return cachedTimeValuePair;
        }
        return chunkReader.next();
    }


    public TSDataType getDataType() {
        return chunkMetaDataList.get(0).getTsDataType();
    }

    public Object getValueInTimeStamp(long timestamp) throws IOException {
        ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(timestamp);
        if (data == null && super.hasNextBatch()) {
            data = super.nextBatch();
        }

        while (data.getTime() < timestamp) {
            data.next();
        }

        if (data.getTime() == timestamp) {
            return data.getValue();
        } else {
            return null;
        }

    }

//    /**
//     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
//     */
//    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
//        this.currentTimestamp = timestamp;
//        if (hasCacheLastTimeValuePair) {
//            if (cachedTimeValuePair.getTimestamp() == timestamp) {
//                hasCacheLastTimeValuePair = false;
//                return cachedTimeValuePair.getValue();
//            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
//                return null;
//            }
//        }
//        if (hasNext()) {
//            cachedTimeValuePair = next();
//            if (cachedTimeValuePair.getTimestamp() == timestamp) {
//                return cachedTimeValuePair.getValue();
//            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
//                hasCacheLastTimeValuePair = true;
//                return null;
//            }
//        }
//        return null;
//    }

    @Override
    protected void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderByTimestamp(chunk);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        long maxTimestamp = chunkMetaData.getEndTime();
        return maxTimestamp >= currentTimestamp;
    }
}
