package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;


public class ChunkReaderWithFilter extends ChunkReader {

    private Filter filter;

    public ChunkReaderWithFilter(Chunk chunk, Filter filter) {
        super(chunk, filter);
        this.filter = filter;
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        if (pageHeader.getMax_timestamp() < getMaxTombstoneTime())
            return false;
        DigestForFilter digest = new DigestForFilter(
                pageHeader.getMin_timestamp(),
                pageHeader.getMax_timestamp(),
                pageHeader.getStatistics().getMinBytebuffer(),
                pageHeader.getStatistics().getMaxBytebuffer(),
                chunkHeader.getDataType());
        return filter.satisfy(digest);
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        if (timeValuePair.getTimestamp() < getMaxTombstoneTime())
            return false;
        return filter.satisfy(timeValuePair);
    }
}
