package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.filenode.QueryStructure;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;
import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhangjinrui on 2018/1/12.
 */
public class SeriesMetadataQuerierIoTDBImpl implements MetadataQuerier {

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final int ROWGROUP_METADATA_CACHE_SIZE = 1000; //TODO: how to specify this value
    private static final int SERIESCHUNK_DESCRIPTOR_CACHE_SIZE = 100000;

    private LRUCache<IntervalFileNode, TsFileMetaData> fileMetadataCache;
    private LRUCache<RowGroupMetadataCacheKey, List<RowGroupMetaData>> rowGroupMetadataInFilesCache;
    private ConcurrentHashMap<QueryJob, Map<IntervalFileNode, List<RowGroupMetaData>>> unsealedRowGroupMetadataMap;

    public void registRowGroupMetadata(QueryJob queryJob, IntervalFileNode intervalFileNode, List<RowGroupMetaData> rowGroupMetaDataList) {
        if (!unsealedRowGroupMetadataMap.containsKey(queryJob)) {
            unsealedRowGroupMetadataMap.put(queryJob, new HashMap<>());
        }
        unsealedRowGroupMetadataMap.get(queryJob).put(intervalFileNode, rowGroupMetaDataList);
    }

    public void unregistRowGroupMetadata(QueryJob queryJob) {
        unsealedRowGroupMetadataMap.remove(queryJob);
    }


    public List<SeriesChunkDescriptor> query(Path path, QueryJob queryJob, QueryStructure queryStructure) {
        List<SeriesChunkDescriptor> seriesChunkDescriptors = new ArrayList<>();
        List<IntervalFileNode> intervalFileNodeList = queryStructure.getBufferwriteDataInFiles();
        if (intervalFileNodeList.size() > 0) {
            for (int i = 0; i < intervalFileNodeList.size() - 1; i++) {
                List<SeriesChunkDescriptor> seriesChunkDescriptorsInOneFile = calSeriesChunkDescriptorInOneFile(path, intervalFileNodeList.get(i));
                seriesChunkDescriptors.addAll(seriesChunkDescriptorsInOneFile);
            }
            seriesChunkDescriptors.addAll(calSeriesChunkDescriptorInUnsealedFile(path, queryJob, intervalFileNodeList.get(intervalFileNodeList.size() - 1)));
        }
        //TODO: add List<Pages> and Docd as SeriesChunkDescriptor ?
        return seriesChunkDescriptors;
    }

    private List<SeriesChunkDescriptor> calSeriesChunkDescriptorInOneFile(Path path, IntervalFileNode intervalFileNode) throws CacheException {
        TsFileMetaData fileMetaData = fileMetadataCache.get(intervalFileNode);

        return null;
    }

    private List<SeriesChunkDescriptor> calSeriesChunkDescriptorInUnsealedFile(Path path, QueryJob queryJob, IntervalFileNode intervalFileNode) {
        return null;
    }

    private List<RowGroupMetaData> loadRowGroupMetadata(String deltaObjectID, ITsRandomAccessFileReader randomAccessFileReader, TsFileMetaData fileMetaData) throws IOException {
        TsDeltaObject deltaObject = fileMetaData.getDeltaObject(deltaObjectID);
        TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
        rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader,
                deltaObject.offset, deltaObject.metadataBlockSize));
        return rowGroupBlockMetaData.getRowGroups();
    }

    private TsFileMetaData getTsFileMetadataFromOneFile(IntervalFileNode intervalFileNode) throws IOException {
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);
        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        return new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
    }

    @Override
    public List<SeriesChunkDescriptor> getSeriesChunkDescriptorList(Path path) throws IOException {
        try {
            return seriesChunkDescriptorCache.get(path);
        } catch (CacheException e) {
            throw new IOException("Get SeriesChunkDescriptorList Error.", e);
        }
    }

    private List<SeriesChunkDescriptor> loadSeriesChunkDescriptorFromOneRowMetadata(Path path, RowGroupMetaData rowGroupMetaData) throws CacheException {
        List<SeriesChunkDescriptor> seriesChunkDescriptorList = new ArrayList<>();
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
            if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getProperties().getMeasurementUID())) {
                seriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData));
            }
        }
        return seriesChunkDescriptorList;
    }

    private SeriesChunkDescriptor generateSeriesChunkDescriptorByMetadata(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        SeriesChunkDescriptor seriesChunkDescriptor = new SeriesChunkDescriptor(
                timeSeriesChunkMetaData.getProperties().getFileOffset(),
                timeSeriesChunkMetaData.getTotalByteSize(),
                timeSeriesChunkMetaData.getProperties().getCompression(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDigest(),
                timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime(),
                timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime(),
                timeSeriesChunkMetaData.getNumRows(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getEnumValues());
        return seriesChunkDescriptor;
    }


    private class RowGroupMetadataCacheKey {

        private static final String SPLIT = "#";
        String deviceId;
        String filePath;

        public int hashCode() {
            StringBuilder stringBuilder = new StringBuilder(deviceId).append(SPLIT).append(filePath);
            return stringBuilder.toString().hashCode();
        }

        public boolean equals(Object o) {
            if (o instanceof RowGroupMetadataCacheKey) {
                RowGroupMetadataCacheKey rowGroupMetadataCacheKey = (RowGroupMetadataCacheKey) o;
                if (rowGroupMetadataCacheKey.deviceId != null && rowGroupMetadataCacheKey.deviceId.equals(deviceId)
                        && rowGroupMetadataCacheKey.filePath != null && rowGroupMetadataCacheKey.filePath.equals(filePath)) {
                    return true;
                }
            }
            return false;
        }
    }
}
