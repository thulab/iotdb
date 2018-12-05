package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class MetadataQuerierByFileImpl implements MetadataQuerier {

    private static final int CHUNK_METADATA_CACHE_SIZE = 100000;

    private TsFileMetaData fileMetaData;

    private LRUCache<Path, List<ChunkMetaData>> chunkMetaDataCache;

    private TsFileSequenceReader tsFileReader;

    public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
        this.tsFileReader = tsFileReader;
        this.fileMetaData = tsFileReader.readFileMetadata();
        chunkMetaDataCache = new LRUCache<Path, List<ChunkMetaData>>(CHUNK_METADATA_CACHE_SIZE) {
            @Override
            public List<ChunkMetaData> loadObjectByKey(Path key) throws IOException {
                return loadChunkMetadata(key);
            }
        };
    }

    @Override
    public List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException {
        return chunkMetaDataCache.get(path);
    }

    @Override
    public TsFileMetaData getWholeFileMetadata() {
        return fileMetaData;
    }


    public void loadChunkMetaDatas(List<Path> paths) throws IOException {
        // get the index information of TsDeviceMetadata

        TreeSet<String> devices = new TreeSet<>();
        for (Path path : paths)
            devices.add(path.getDeviceToString());

        Map<Path, List<ChunkMetaData>> tempChunkMetaDatas = new HashMap<>();

        // get all TsDeviceMetadataIndex by string order
        for (String device : devices) {
            TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(device);
            FileChannel channel = tsFileReader.getChannel();
            channel.position(index.getOffset());
            ByteBuffer buffer = ByteBuffer.allocate(index.getLen());
            channel.read(buffer);
            buffer.flip();
            TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(buffer);

            // d1
            for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroups()) {

                // s1, s2
                for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {

                    // d1.s1, d1.s2, d2.s3, d2.s4
                    for(Path path: paths) {

                        // d1.s1, d1.s2
                        if(chunkGroupMetaData.getDeviceID().equals(path.getDeviceToString())
                                && chunkMetaData.getMeasurementUID().equals(path.getMeasurementToString())) {
                            if(!tempChunkMetaDatas.containsKey(path))
                                tempChunkMetaDatas.put(path, new ArrayList<>());
                            tempChunkMetaDatas.get(path).add(chunkMetaData);
                        }
                    }
                }
            }
        }

        for(Map.Entry<Path, List<ChunkMetaData>> entry: tempChunkMetaDatas.entrySet())
            chunkMetaDataCache.put(entry.getKey(), entry.getValue());

    }


    private List<ChunkMetaData> loadChunkMetadata(Path path) throws IOException {

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
