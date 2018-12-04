package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
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
    private int currentSeriesChunkIndex;

    private DynamicOneColumnData data = null;

    public SeriesReaderByTimestamp(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(chunkLoader, chunkMetaDataList);
        currentSeriesChunkIndex = 0;
        currentTimestamp = Long.MIN_VALUE;
    }

    public TSDataType getDataType() {
        return chunkMetaDataList.get(0).getTsDataType();
    }

    public Object getValueInTimestampV3(long timestamp) throws IOException {
        this.currentTimestamp = timestamp;

        if (chunkReader == null) {
            if (!constructNextSatisfiedChunkReader())
                return null;
        }

        if (data == null) {
            if (chunkReader.hasNextBatch())
                data = chunkReader.nextBatch();
            else
                return null;
        }

        while (data != null) {
            while (data.hasNext()) {
                if (data.getTime() < timestamp)
                    data.next();
                else
                    break;
            }
            if (data.hasNext() && data.getTime() == timestamp)
                return data.getValue();
            else if (data.hasNext())
                return null;
            else if (chunkReader.hasNextBatch()) // data does not has next
                data = nextBatch();
            else {
                if (!constructNextSatisfiedChunkReader())
                    return null;
            }
        }

        return null;
    }

    private boolean constructNextSatisfiedChunkReader() throws IOException {
        while (currentChunkIndex < chunkMetaDataList.size()) {
            ChunkMetaData chunkMetaData = chunkMetaDataList.get(currentChunkIndex++);
            if (chunkSatisfied(chunkMetaData)) {
                initChunkReader(chunkMetaData);
                ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp);
                return true;
            }
        }
        return false;
    }

    @Override
    protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        this.chunkReader = new ChunkReaderByTimestamp(chunk);
        this.chunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
        long maxTimestamp = chunkMetaData.getEndTime();
        return maxTimestamp >= currentTimestamp;
    }


    // ============= These methods below are deprecated ==========================

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
        while (currentSeriesChunkIndex < chunkMetaDataList.size()) {
            if (!chunkReaderInitialized) {
                ChunkMetaData chunkMetaData = chunkMetaDataList.get(currentSeriesChunkIndex);
                // maxTime >= currentTime
                if (chunkSatisfied(chunkMetaData)) {
                    initChunkReader(chunkMetaData);
                    ((ChunkReaderByTimestamp) chunkReader).setCurrentTimestamp(currentTimestamp);
                    chunkReaderInitialized = true;
                    currentSeriesChunkIndex++;
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

    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        this.currentTimestamp = timestamp;
        if (hasCacheLastTimeValuePair) {
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                hasCacheLastTimeValuePair = false;
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                return null;
            }
        }
        if (hasNext()) {
            cachedTimeValuePair = next();
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                hasCacheLastTimeValuePair = true;
                return null;
            }
        }
        return null;
    }
}
