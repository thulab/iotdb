package cn.edu.tsinghua.tsfile.timeseries.write.io;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.*;
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

    private FileOutputStream out;
    protected List<ChunkGroupMetaData> chunkGroupMetaData = new ArrayList<>();
    private ChunkGroupMetaData currentChunkGroupMetaData;
    private ChunkMetaData currentChunkMetaData;

    /**
     * for writing a new tsfile.
     *
     * @param file be used to output written data
     * @throws IOException if I/O error occurs
     */
    public TsFileIOWriter(File file) throws IOException {
        this.out = new FileOutputStream(file);
        startFile();
    }


    /**
     * Writes given bytes to output stream.
     * This method is called when total memory size exceeds the row group size
     * threshold.
     *
     * @param bytes - data of several pages which has been packed
     * @throws IOException if an I/O error occurs.
     */
    public void writeBytesToStream(PublicBAOS bytes) throws IOException {
        bytes.writeTo(out);
    }

    private void startFile() throws IOException {
        out.write(magicStringBytes);
    }

    /**
     * start a {@linkplain ChunkGroupMetaData ChunkGroupMetaData}.
     *
     * @param deviceId delta object id
     * @param dataSize      the serialized size of all chunks
     * @return the serialized size of ChunkGroupFooter
     */
    public ChunkGroupFooter startFlushChunkGroup(String deviceId, long dataSize, int numberOfChunks) throws IOException {
        LOG.debug("start row group:{}, file position {}", deviceId, out.getChannel().position());
        currentChunkGroupMetaData = new ChunkGroupMetaData(deviceId, new ArrayList<>());
        ChunkGroupFooter header = new ChunkGroupFooter(deviceId, dataSize, numberOfChunks);
        LOG.debug("finishing writing row group header {}, file position {}", header, out.getChannel().position());
        return header;
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
        LOG.debug("start series chunk:{}, file position {}", descriptor, out.getChannel().position());

        currentChunkMetaData = new ChunkMetaData(descriptor.getMeasurementId(), tsDataType, out.getChannel().position(), minTime, maxTime);

        ChunkHeader header = new ChunkHeader(descriptor.getMeasurementId(), datasize, tsDataType, compressionCodecName, encodingType, numOfPages);
        header.serializeTo(out);
        LOG.debug("finish series chunk:{} header, file position {}", header, out.getChannel().position());

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

    public void endChunkGroup(long memSize, ChunkGroupFooter chunkGroupFooter) throws IOException {
        chunkGroupFooter.serializeTo(out);
        chunkGroupMetaData.add(currentChunkGroupMetaData);
        LOG.debug("end row group:{}", currentChunkGroupMetaData);
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
    	// get all TimeSeriesMetadatas of this TsFile
        Map<String, MeasurementSchema> schemaDescriptors = schema.getAllMeasurementSchema();
        LOG.debug("get time series list:{}", schemaDescriptors);
        // clustering chunkGroupMetadata and build the range
        Map<String,TsDeviceMetadataIndex> tsDeviceMetadataIndexMap = new HashMap<>();
        String currentDevice;
        TsDeviceMetadata currentTsDeviceMetadata;

        LinkedHashMap<String, TsDeviceMetadata> tsDeviceMetadataMap = new LinkedHashMap<>();

        for (ChunkGroupMetaData chunkGroupMetaData : this.chunkGroupMetaData) {
            currentDevice = chunkGroupMetaData.getDeviceID();
            if (!tsDeviceMetadataMap.containsKey(currentDevice)) {
                TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
                tsDeviceMetadataMap.put(currentDevice, tsDeviceMetadata);
            }
            tsDeviceMetadataMap.get(currentDevice).addChunkGroupMetaData(chunkGroupMetaData);
        }
        Iterator<Map.Entry<String, TsDeviceMetadata>> iterator = tsDeviceMetadataMap.entrySet().iterator();

        /* start time for a delta object */
        long startTime;

        /* end time for a delta object */
        long endTime;

        /*offset for the grouping delta object metadata*/
        long offset;

        while (iterator.hasNext()) {
            startTime = Long.MAX_VALUE;
            endTime = Long.MIN_VALUE;

            Map.Entry<String, TsDeviceMetadata> entry = iterator.next();
            currentTsDeviceMetadata = entry.getValue();

            for (ChunkGroupMetaData chunkGroupMetaData : currentTsDeviceMetadata.getChunkGroups()) {
                for (ChunkMetaData chunkMetaData : chunkGroupMetaData
                        .getChunkMetaDataList()) {
					// update startTime and endTime
                    startTime = Long.min(startTime, chunkMetaData.getStartTime());
                    endTime = Long.max(endTime, chunkMetaData.getEndTime());
                }
            }
            // flush tsChunkGroupBlockMetaDatas in order
            currentTsDeviceMetadata.setStartTime(startTime);
            currentTsDeviceMetadata.setEndTime(endTime);

            offset = out.getChannel().position();
            int size = currentTsDeviceMetadata.serializeTo(out);

            TsDeviceMetadataIndex tsDeviceMetadataIndex = new TsDeviceMetadataIndex(offset,size,startTime,endTime);
            tsDeviceMetadataIndexMap.put(entry.getKey(),tsDeviceMetadataIndex);
        }

        TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeviceMetadataIndexMap, schemaDescriptors,
                TSFileConfig.currentVersion);

        long footerIndex = out.getChannel().position();
        LOG.debug("start to flush the footer,file pos:{}", footerIndex);
        int size = tsFileMetaData.serializeTo(out);
        LOG.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getChannel().position());
        ReadWriteIOUtils.write(size, out);//write the size of the file metadata.
        out.write(magicStringBytes);
        out.close();
        LOG.info("output stream is closed");
    }

    /**
     * get the length of normal OutputStream.
     *
     * @return - length of normal OutputStream
     * @throws IOException if I/O error occurs
     */
    public long getPos() throws IOException {
        return out.getChannel().position();
    }

}
