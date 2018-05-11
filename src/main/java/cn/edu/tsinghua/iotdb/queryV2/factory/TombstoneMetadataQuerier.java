package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.tombstone.Tombstone;
import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TombstoneMetadataQuerier extends SimpleMetadataQuerierForMerge {

    /**
     * Tombstones of this file.
     */
    private List<Tombstone> tombstones;

    public TombstoneMetadataQuerier(String filePath, List<Tombstone> tombstoneList) throws IOException {
        super(filePath);
        this.tombstones = tombstoneList;
    }

    /**
     * Compute the maxTombstoneTime of each series and put it into every EncodedSeriesChunkDescriptor.
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public List<EncodedSeriesChunkDescriptor> getSeriesChunkDescriptorList(Path path) throws IOException {
        long maxTombstoneTime;
        try {
            List<RowGroupMetaData> rowGroupMetaDataList = rowGroupMetadataCache.get(path.getDeltaObjectToString());
            List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = new ArrayList<>();
            for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
                // find max tombstone time of this rowgroup
                maxTombstoneTime = 0;
                for(Tombstone tombstone : tombstones) {
                    if(tombstone.executeTimestamp > rowGroupMetaData.getWrittenTime())
                        maxTombstoneTime = maxTombstoneTime > tombstone.deleteTimestamp ? maxTombstoneTime : tombstone.deleteTimestamp;
                }
                List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
                    if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getProperties().getMeasurementUID())) {
                        EncodedSeriesChunkDescriptor chunkDescriptor = generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData);
                        chunkDescriptor.setMaxTombstoneTime(maxTombstoneTime);
                        encodedSeriesChunkDescriptorList.add(chunkDescriptor);
                    }
                }
            }
            return encodedSeriesChunkDescriptorList;
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }
}
