package cn.edu.tsinghua.iotdb.engine.bufferwrite;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadata;
import cn.edu.tsinghua.tsfile.utils.Pair;
import cn.edu.tsinghua.tsfile.write.writer.DefaultTsFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import cn.edu.tsinghua.tsfile.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;


public class BufferWriteRestoreManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteRestoreManager.class);
    private static final int TS_METADATA_BYTE_SIZE = 4;
    private static final int TS_POSITION_BYTE_SIZE = 8;

    private static final String RESTORE_SUFFIX = ".restore";
    private static final String DEFAULT_MODE = "rw";
    private Map<String, Map<String, List<ChunkMetaData>>> metadatas;
    private List<ChunkGroupMetaData> appendRowGroupMetadatas;
//    private BufferIO bufferWriteIO;
    /**
     * unsealed data file
     */
    private String insertFilePath;
    /**
     * corresponding index file
     */
    private String restoreFilePath;
    private String processorName;

    private boolean isNewResource = false;

    BufferWriteRestoreManager(String processorName, String insertFilePath) throws IOException {
        this.insertFilePath = insertFilePath;
        this.restoreFilePath = insertFilePath + RESTORE_SUFFIX;
        this.processorName = processorName;
        this.metadatas = new HashMap<>();
        this.appendRowGroupMetadatas = new ArrayList<>();
        //recover();
    }


    /**
     *
     * @return an opened BufferIO. Therefore, you MUST close the BufferIO manually.
     * @throws IOException
     */
     BufferIO recover() throws IOException {
        File insertFile = new File(insertFilePath);
        File restoreFile = new File(restoreFilePath);
        if (insertFile.exists() && restoreFile.exists()) {
            // read restore file
            Pair<Long, List<ChunkGroupMetaData>> restoreInfo = readRestoreInfo();
            long position = restoreInfo.left;
            List<ChunkGroupMetaData> existedMetadatas = restoreInfo.right;
            // cut off tsfile
            FileOutputStream fileOutputStream = new FileOutputStream(insertFile, true);
            fileOutputStream.getChannel().truncate(position);
            fileOutputStream.getChannel().position();
            // recovery the BufferWriteIO
            BufferIO bufferWriteIO = new BufferIO(new DefaultTsFileOutput(fileOutputStream), existedMetadatas);
            // recovery the metadata
            recoverMetadata(existedMetadatas);
            LOGGER.info(
                    "Recover the bufferwrite processor {}, the tsfile seriesPath is {}, the position of last flush is {}, the size of rowGroupMetadata is {}",
                    processorName, insertFilePath, position, existedMetadatas.size());
            isNewResource = false;
            return bufferWriteIO;
        } else {
            if(!insertFile.delete()) {
                LOGGER.info("remove unsealed tsfile {} failed.", insertFilePath);
            }
            if(!restoreFile.delete()) {
                LOGGER.info("remove unsealed tsfile restore file {} failed.", restoreFilePath);
            }
            DefaultTsFileOutput defaultTsFileOutput = new DefaultTsFileOutput(new FileOutputStream(insertFile));
            BufferIO bufferWriteIO = new BufferIO(defaultTsFileOutput, new ArrayList<>());
            isNewResource = true;
            writeRestoreInfo(bufferWriteIO.getPos(), Collections.EMPTY_LIST);
            return bufferWriteIO;
        }
    }

    private void recoverMetadata(List<ChunkGroupMetaData> rowGroupMetaDatas) {
        //TODO it is better if we can consider the problem caused by deletion and re-create time series here.
        for (ChunkGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
            String deltaObjectId = rowGroupMetaData.getDeviceID();
            if (!metadatas.containsKey(deltaObjectId)) {
                metadatas.put(deltaObjectId, new HashMap<>());
            }
            for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
                String measurementId = chunkMetaData.getMeasurementUID();
                if (!metadatas.get(deltaObjectId).containsKey(measurementId)) {
                    metadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
                }
                metadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
            }
        }
    }


    private void writeRestoreInfo(long lastPosition, List<ChunkGroupMetaData> appendRowGroupMetaDatas) throws IOException {
        // List<ChunkGroupMetaData> appendRowGroupMetaDatas = bufferWriteIO.getAppendedRowGroupMetadata();

        //TODO: no need to create a TsRowGroupBlockMetadata, flush RowGroupMetadata one by one is ok
        TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
        tsDeviceMetadata.setChunkGroupMetadataList(appendRowGroupMetaDatas);
        RandomAccessFile out = null;
        out = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
        try {
            if (out.length() > 0) {
                out.seek(out.length() - TS_POSITION_BYTE_SIZE);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            tsDeviceMetadata.serializeTo(baos);
            // write metadata size using int
            int metadataSize = baos.size();
            out.write(BytesUtils.intToBytes(metadataSize));
            // write metadata
            out.write(baos.toByteArray());
            // write tsfile position using byte[8] which is a long
            byte[] lastPositionBytes = BytesUtils.longToBytes(lastPosition);
            out.write(lastPositionBytes);
        } finally {
            out.close();
        }
    }

    /**
     *
     * @return a pair, whose left Long value is the tail position of the last complete Chunk Group in the unsealed file's position,
     *          and the right List value is the ChunkGroupMetadata of all complete Chunk Group in the same file.
     * @throws IOException if errors when reading restoreFile.
     */
    Pair<Long, List<ChunkGroupMetaData>> readRestoreInfo() throws IOException {
        byte[] lastPostionBytes = new byte[TS_POSITION_BYTE_SIZE];
        List<ChunkGroupMetaData> groupMetaDatas = new ArrayList<>();
        RandomAccessFile randomAccessFile = null;
        randomAccessFile = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
        try {
            long fileLength = randomAccessFile.length();
            // read tsfile position
            long point = randomAccessFile.getFilePointer();
            while (point + TS_POSITION_BYTE_SIZE < fileLength) {
                byte[] metadataSizeBytes = new byte[TS_METADATA_BYTE_SIZE];
                randomAccessFile.read(metadataSizeBytes);
                int metadataSize = BytesUtils.bytesToInt(metadataSizeBytes);
                byte[] thriftBytes = new byte[metadataSize];
                randomAccessFile.read(thriftBytes);
                ByteArrayInputStream inputStream = new ByteArrayInputStream(thriftBytes);
                TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(inputStream);
                groupMetaDatas.addAll(tsDeviceMetadata.getChunkGroups());
                point = randomAccessFile.getFilePointer();
            }
            // read the tsfile position information using byte[8] which is a long.
            randomAccessFile.read(lastPostionBytes);
            long lastPosition = BytesUtils.bytesToLong(lastPostionBytes);
            return new Pair<>(lastPosition, groupMetaDatas);
        } finally {
            randomAccessFile.close();
        }
    }

    /**
     * get chunks' metadata from memory
     * @param deltaObjectId the device id
     * @param measurementId the sensor id
     * @param dataType the value type
     * @return chunks' metadata
     */
    List<ChunkMetaData> getInsertMetadatas(String deltaObjectId, String measurementId,
                                                  TSDataType dataType) {
        List<ChunkMetaData> chunkMetaDatas = new ArrayList<>();
        if (metadatas.containsKey(deltaObjectId)) {
            if (metadatas.get(deltaObjectId).containsKey(measurementId)) {
                for (ChunkMetaData chunkMetaData : metadatas.get(deltaObjectId).get(measurementId)) {
                    // filter: if a device'sensor is defined as float type, and data has been persistent.
                    // Then someone deletes the timeseries and recreate it with Int type. We have to ignore all the stale data.
                    if (dataType.equals(chunkMetaData.getTsDataType())) {
                        chunkMetaDatas.add(chunkMetaData);
                    }
                }
            }
        }
        return chunkMetaDatas;
    }

    String getInsertFilePath() {
        return insertFilePath;
    }

    String getRestoreFilePath() {
        return restoreFilePath;
    }

    boolean isNewResource() {
        return isNewResource;
    }

    void setNewResource(boolean isNewResource) {
        this.isNewResource = isNewResource;
    }


    public void flush(long position, List<ChunkGroupMetaData> appendChunkGroupMetadta) throws IOException {
            writeRestoreInfo(position, appendChunkGroupMetadta);
            appendRowGroupMetadatas.addAll(appendChunkGroupMetadta);
    }

    /**
     * add all appendChunkGroupMetadatas into memory.
     * After calling this method, other classes can read these metadata.
     */
    void appendMetadata() {
        if (!appendRowGroupMetadatas.isEmpty()) {
            for (ChunkGroupMetaData rowGroupMetaData : appendRowGroupMetadatas) {
                for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
                    addInsertMetadata(rowGroupMetaData.getDeviceID(),
                            chunkMetaData.getMeasurementUID(), chunkMetaData);
                }
            }
            appendRowGroupMetadatas.clear();
        }
    }

    private void addInsertMetadata(String deltaObjectId, String measurementId, ChunkMetaData chunkMetaData) {
        if (!metadatas.containsKey(deltaObjectId)) {
            metadatas.put(deltaObjectId, new HashMap<>());
        }
        if (!metadatas.get(deltaObjectId).containsKey(measurementId)) {
            metadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
        }
        metadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
    }



    void deleteRestoreFile() {
        try {
            Files.delete(Paths.get(restoreFilePath));
        } catch (IOException e) {
            LOGGER.info("delete restore file {} failed, because {}.", restoreFilePath, e.getMessage());
        }
    }
}
