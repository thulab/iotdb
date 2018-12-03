package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;

import java.io.IOException;


public class ChunkReaderByTimestamp extends ChunkReader {

    private long currentTimestamp;

    public ChunkReaderByTimestamp(Chunk chunk) {
        super(chunk);
    }


    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        long maxTimestamp = pageHeader.getMax_timestamp();
        //If minTimestamp > currentTimestamp, this page should NOT be skipped
        return maxTimestamp >= currentTimestamp && maxTimestamp >= getMaxTombstoneTime();
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return timeValuePair.getTimestamp() >= currentTimestamp && timeValuePair.getTimestamp() > getMaxTombstoneTime();
    }

    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
        if (hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() < currentTimestamp) {
            hasCachedTimeValuePair = false;
        }
    }

    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        setCurrentTimestamp(timestamp);
        if (hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() == timestamp) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair.getValue();
        }
        while (hasNext()) {
            cachedTimeValuePair = next();
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                hasCachedTimeValuePair = true;
                return null;
            }
        }
        return null;
    }

}
