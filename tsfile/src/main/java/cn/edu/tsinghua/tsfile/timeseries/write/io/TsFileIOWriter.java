package cn.edu.tsinghua.tsfile.timeseries.write.io;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.*;
import cn.edu.tsinghua.tsfile.file.MetaMarker;
import cn.edu.tsinghua.tsfile.file.footer.ChunkGroupFooter;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory
 * to output stream.
 *
 * @author kangrong
 */
public class TsFileIOWriter {

    public static final byte[] magicStringBytes;
    private static final Logger LOG = LoggerFactory.getLogger(TsFileIOWriter.class);

    static {
        magicStringBytes = BytesUtils.StringToBytes(TSFileConfig.MAGIC_STRING);
    }

    private TsFileOutput out;
    protected List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
    private ChunkGroupMetaData currentChunkGroupMetaData;
    private ChunkMetaData currentChunkMetaData;

    /**
     * for writing a new tsfile.
     *
     * @param file be used to output written data
     * @throws IOException if I/O error occurs
     */
    public TsFileIOWriter(File file) throws IOException {
        this.out = new DefaultTsFileOutput(file);
        startFile();
    }


    /**
     * Writes given bytes to output stream.
     * This method is called when total memory size exceeds the chunk group size
     * threshold.
     *
     * @param bytes - data of several pages which has been packed
     * @throws IOException if an I/O error occurs.
     */
    public void writeBytesToStream(PublicBAOS bytes) throws IOException {
        bytes.writeTo(out.wrapAsStream());
    }

    private void startFile() throws IOException {
        out.write(magicStringBytes);
    }

    /**
     * start a {@linkplain ChunkGroupMetaData ChunkGroupMetaData}.
     *
     * @param deviceId device id
     * @param dataSize the serialized size of all chunks
     * @return the serialized size of ChunkGroupFooter
     */
    public ChunkGroupFooter startFlushChunkGroup(String deviceId, long dataSize, int numberOfChunks) throws IOException {
        LOG.debug("start chunk group:{}, file position {}", deviceId, out.getPosition());
        currentChunkGroupMetaData = new ChunkGroupMetaData(deviceId, new ArrayList<>());
        ChunkGroupFooter footer = new ChunkGroupFooter(deviceId, dataSize, numberOfChunks);
        LOG.debug("finishing writing chunk group header {}, file position {}", footer, out.getPosition());
        return footer;
    }

    /**
     * start a {@linkplain ChunkMetaData ChunkMetaData}.
     *
     * @param descriptor           - measurement of this time series
     * @param compressionCodecName - compression name of this time series
     * @param tsDataType           - data type
     * @param statistics           - statistic of the whole series
     * @param maxTime              - maximum timestamp of the whole series in this stage
     * @param minTime              - minimum timestamp of the whole series in this stage
     * @param datasize             -  the serialized size of all pages
     * @return the serialized size of CHunkHeader
     * @throws IOException if I/O error occurs
     */
    public int startFlushChunk(MeasurementSchema descriptor, CompressionType compressionCodecName,
                               TSDataType tsDataType, TSEncoding encodingType, Statistics<?> statistics, long maxTime, long minTime, int datasize, int numOfPages) throws IOException {
        LOG.debug("start series chunk:{}, file position {}", descriptor, out.getPosition());

        currentChunkMetaData = new ChunkMetaData(descriptor.getMeasurementId(), tsDataType, out.getPosition(), minTime, maxTime);

        ChunkHeader header = new ChunkHeader(descriptor.getMeasurementId(), datasize, tsDataType, compressionCodecName, encodingType, numOfPages);
        header.serializeTo(out.wrapAsStream());
        LOG.debug("finish series chunk:{} header, file position {}", header, out.getPosition());

        TsDigest tsDigest = new TsDigest();
        Map<String, ByteBuffer> statisticsMap = new HashMap<>();
        // TODO add your statistics
        statisticsMap.put(StatisticConstant.MAX_VALUE, ByteBuffer.wrap(statistics.getMaxBytes()));
        statisticsMap.put(StatisticConstant.MIN_VALUE, ByteBuffer.wrap(statistics.getMinBytes()));
        statisticsMap.put(StatisticConstant.FIRST, ByteBuffer.wrap(statistics.getFirstBytes()));
        statisticsMap.put(StatisticConstant.SUM, ByteBuffer.wrap(statistics.getSumBytes()));
        statisticsMap.put(StatisticConstant.LAST, ByteBuffer.wrap(statistics.getLastBytes()));
        tsDigest.setStatistics(statisticsMap);

        currentChunkMetaData.setDigest(tsDigest);

        return header.getSerializedSize();
    }


    public void endChunk(long totalValueCount) {
        currentChunkMetaData.setNumOfPoints(totalValueCount);
        currentChunkGroupMetaData.addTimeSeriesChunkMetaData(currentChunkMetaData);
        LOG.debug("end series chunk:{},totalvalue:{}", currentChunkMetaData, totalValueCount);
        currentChunkMetaData = null;
    }

    public void endChunkGroup(ChunkGroupFooter chunkGroupFooter) throws IOException {
        chunkGroupFooter.serializeTo(out.wrapAsStream());
        chunkGroupMetaDataList.add(currentChunkGroupMetaData);
        LOG.debug("end chunk group:{}", currentChunkGroupMetaData);
        currentChunkGroupMetaData = null;
    }

    /**
     * write {@linkplain TsFileMetaData TSFileMetaData} to output stream and
     * close it.
     *
     * @param schema FileSchema
     * @throws IOException if I/O error occurs
     */
    public void endFile(FileSchema schema) throws IOException {

        // serialize the Separator of MetaData and ChunkGroups
        ReadWriteIOUtils.write(MetaMarker.Separator, out.wrapAsStream());

        // get all measurementSchema of this TsFile
        Map<String, MeasurementSchema> schemaDescriptors = schema.getAllMeasurementSchema();
        LOG.debug("get time series list:{}", schemaDescriptors);

        Map<String, TsDeviceMetadataIndex> tsDeviceMetadataIndexMap = flushTsDeviceMetaDataAndGetIndex(this.chunkGroupMetaDataList);

        TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeviceMetadataIndexMap, schemaDescriptors,
                TSFileConfig.currentVersion);

        long footerIndex = out.getPosition();
        LOG.debug("start to flush the footer,file pos:{}", footerIndex);

        // write TsFileMetaData
        int size = tsFileMetaData.serializeTo(out.wrapAsStream());
        LOG.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPosition());

        // write TsFileMetaData size
        ReadWriteIOUtils.write(size, out.wrapAsStream());//write the size of the file metadata.

        // write magic string
        out.write(magicStringBytes);

        // close file
        out.close();
        LOG.info("output stream is closed");
    }


    /**
     * 1. group chunkGroupMetaDataList to TsDeviceMetadata
     * 2. flush TsDeviceMetadata
     * 3. get TsDeviceMetadataIndex
     *
     * @param chunkGroupMetaDataList all chunk group metadata in memory
     * @return TsDeviceMetadataIndex in TsFileMetaData
     */
    private Map<String, TsDeviceMetadataIndex> flushTsDeviceMetaDataAndGetIndex(List<ChunkGroupMetaData> chunkGroupMetaDataList) throws IOException {

        Map<String, TsDeviceMetadataIndex> tsDeviceMetadataIndexMap = new HashMap<>();

        long offset; /*offset for the flushing TsDeviceMetadata*/

        TsDeviceMetadata currentTsDeviceMetadata;

        for (Map.Entry<String, TsDeviceMetadata> entry : getAllTsDeviceMetadata(chunkGroupMetaDataList).entrySet()) {
            // update statistics in TsDeviceMetadata
            currentTsDeviceMetadata = entry.getValue();

            // flush tsChunkGroupBlockMetaData
            offset = out.getPosition();
            int size = currentTsDeviceMetadata.serializeTo(out.wrapAsStream());

            TsDeviceMetadataIndex tsDeviceMetadataIndex = new TsDeviceMetadataIndex(offset, size, currentTsDeviceMetadata);
            tsDeviceMetadataIndexMap.put(entry.getKey(), tsDeviceMetadataIndex);
        }

        return tsDeviceMetadataIndexMap;
    }


    /**
     * group all chunk group metadata by device
     *
     * @param chunkGroupMetaDataList all chunk group metadata
     * @return TsDeviceMetadata of all devices
     */
    private LinkedHashMap<String, TsDeviceMetadata> getAllTsDeviceMetadata(List<ChunkGroupMetaData> chunkGroupMetaDataList) {
        String currentDevice;
        LinkedHashMap<String, TsDeviceMetadata> tsDeviceMetadataMap = new LinkedHashMap<>();

        for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
            currentDevice = chunkGroupMetaData.getDeviceID();

            if (!tsDeviceMetadataMap.containsKey(currentDevice)) {
                TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
                tsDeviceMetadataMap.put(currentDevice, tsDeviceMetadata);
            }
            tsDeviceMetadataMap.get(currentDevice).addChunkGroupMetaData(chunkGroupMetaData);
        }
        return tsDeviceMetadataMap;
    }

    /**
     * get the length of normal OutputStream.
     *
     * @return - length of normal OutputStream
     * @throws IOException if I/O error occurs
     */
    public long getPos() throws IOException {
        return out.getPosition();
    }

}
