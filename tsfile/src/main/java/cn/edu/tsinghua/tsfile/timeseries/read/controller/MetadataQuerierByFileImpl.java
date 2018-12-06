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

        // group measurements by device
        TreeMap<String, Set<String>> device_measurementsMap = new TreeMap<>();
        for (Path path : paths) {
            if (!device_measurementsMap.containsKey(path.getDevice()))
                device_measurementsMap.put(path.getDevice(), new HashSet<>());
            device_measurementsMap.get(path.getDevice()).add(path.getMeasurement());
        }

        Map<Path, List<ChunkMetaData>> tempChunkMetaDatas = new HashMap<>();

        // get all TsDeviceMetadataIndex by string order
        for (Map.Entry<String, Set<String>> device_measurements : device_measurementsMap.entrySet()) {

            // d1
            String selectedDevice = device_measurements.getKey();
            // s1, s2, s3
            Set<String> selectedMeasurements = device_measurements.getValue();

            // get the index information of TsDeviceMetadata
            TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(selectedDevice);
            TsDeviceMetadata tsDeviceMetadata = tsFileReader.readTsDeviceMetaData(index);

            // d1
            for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroups()) {

                // s1, s2
                for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {

                    String currentMeasurement = chunkMetaData.getMeasurementUID();

                    if (selectedMeasurements.contains(currentMeasurement)) {
                        Path path = new Path(selectedDevice, currentMeasurement);
                        if (!tempChunkMetaDatas.containsKey(path))
                            tempChunkMetaDatas.put(path, new ArrayList<>());
                        tempChunkMetaDatas.get(path).add(chunkMetaData);
                    }
                }
            }
        }

        int count = 0;
        for (Map.Entry<Path, List<ChunkMetaData>> entry : tempChunkMetaDatas.entrySet()) {
            chunkMetaDataCache.put(entry.getKey(), entry.getValue());
            count++;
            if (count == CHUNK_METADATA_CACHE_SIZE)
                break;
        }

    }


    private List<ChunkMetaData> loadChunkMetadata(Path path) throws IOException {

        // get the index information of TsDeviceMetadata
        TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(path.getDevice());

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
                if (path.getMeasurement().equals(chunkMetaData.getMeasurementUID())) {
                    chunkMetaDataList.add(chunkMetaData);
                }
            }
        }
        return chunkMetaDataList;
    }

}
