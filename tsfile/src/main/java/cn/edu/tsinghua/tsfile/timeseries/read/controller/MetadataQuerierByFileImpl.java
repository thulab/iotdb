package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class MetadataQuerierByFileImpl implements MetadataQuerier {

    private static final int SERIESCHUNK_DESCRIPTOR_CACHE_SIZE = 100000;

    private TsFileMetaData fileMetaData;

    private LRUCache<Path, List<ChunkMetaData>> seriesChunkDescriptorCache;

    private TsFileSequenceReader tsFileReader;

    public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
        this.tsFileReader = tsFileReader;
        this.fileMetaData = tsFileReader.readFileMetadata();
        seriesChunkDescriptorCache = new LRUCache<Path, List<ChunkMetaData>>(SERIESCHUNK_DESCRIPTOR_CACHE_SIZE) {

            @Override
            public List<ChunkMetaData> loadObjectByKey(Path key) throws IOException {
                return loadSeriesChunkDescriptor(key);
            }
        };
    }

    @Override
    public List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException {
        try {
            return seriesChunkDescriptorCache.get(path);
        } catch (CacheException e) {
            throw new IOException(String.format("Get SeriesChunkDescriptorList for Path[%s] Error.", path), e);
        }
    }

    @Override
    public TsFileMetaData getWholeFileMetadata() {
        return fileMetaData;
    }

    private List<ChunkMetaData> loadSeriesChunkDescriptor(Path path) throws IOException {

        // get the index information of TsDeviceMetadata
        TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(path.getDeviceToString());

        // read TsDeviceMetadata from file
        FileChannel channel = tsFileReader.getChannel();
        channel.position(index.getOffset());
        ByteBuffer buffer = ByteBuffer.allocate(index.getLen());
        channel.read(buffer);
        buffer.flip();

        TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(buffer);

        // get all ChunkMetaData of this path included in all ChunkGroups of this device
        List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
        for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroups()) {
            List<ChunkMetaData> chunkMetaDataListInOneChunkGroup = chunkGroupMetaData.getChunkMetaDataList();
            for (ChunkMetaData chunkMetaData : chunkMetaDataListInOneChunkGroup) {
                if (path.getMeasurementToString().equals(chunkMetaData.getMeasurementUID())) {
                    chunkMetaDataList.add(chunkMetaData);
                }
            }
        }
        return chunkMetaDataList;
    }

}
