package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.getValidFiles;
import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.restoreFilePathName;
import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.unseqTsFilePathName;
import static cn.edu.tsinghua.iotdb.performance.ReaderCreator.getTsFileMetadata;
import static cn.edu.tsinghua.iotdb.performance.ReaderCreator.getUnSeqFileMetaData;

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class WriteSample {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteSample.class);

    private static String mergeOutPutFolder;
    private static String fileFolderName;
    private static String unSeqFilePath;


    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> unSeqFileMetaData;
    private static Map<String, Pair<Long, Long>> unSeqFileDeltaObjectTimeRangeMap;

    public static void main(String args[]) throws WriteProcessException, IOException {
        fileFolderName = args[0];
        mergeOutPutFolder = fileFolderName + "/merge/";
        unSeqFilePath = fileFolderName + "/" + unseqTsFilePathName;

        unSeqFileMetaData = getUnSeqFileMetaData(unSeqFilePath);

        initUnSeqFileStatistics();

        executeMerge();
    }

    private static void initUnSeqFileStatistics() {
        unSeqFileDeltaObjectTimeRangeMap = new HashMap<>();
        for (Map.Entry<String, Map<String, List<TimeSeriesChunkMetaData>>> entry : unSeqFileMetaData.entrySet()) {
            String unSeqDeltaObjectId = entry.getKey();
            long maxTime = Long.MIN_VALUE, minTime = Long.MAX_VALUE;
            for (List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList : entry.getValue().values()) {
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
                    minTime = Math.min(minTime, timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime());
                    maxTime = Math.max(maxTime, timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime());
                }
            }
            unSeqFileDeltaObjectTimeRangeMap.put(unSeqDeltaObjectId, new Pair<>(minTime, maxTime));
        }
    }

    private static void executeMerge() throws IOException, WriteProcessException {
        Pair<Boolean, File[]> validFiles = getValidFiles(fileFolderName);
        if (!validFiles.left) {
            LOGGER.info("There exists error in given file folder");
            return;
        }
        File[] files = validFiles.right;

        long allFileMergeStartTime = System.currentTimeMillis();
        for (File file : files) {
            if (!file.isDirectory() && !file.getName().endsWith(restoreFilePathName) &&
                    !file.getName().equals(unseqTsFilePathName) && !file.getName().endsWith("Store")) {
                System.out.println(String.format("---- merge process begin, current merge file is %s.", file.getName()));

                long oneFileMergeStartTime = System.currentTimeMillis();
                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);

                // construct file writer for the merge process
                FileSchema fileSchema = new FileSchema();
                File outPutFile = new File(mergeOutPutFolder + file.getName());
                for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                    fileSchema.registerMeasurement(new MeasurementDescriptor(timeSeriesMetadata.getMeasurementUID(), timeSeriesMetadata.getType(),
                            getEncodingByDataType(timeSeriesMetadata.getType())));
                }
                TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema, TSFileDescriptor.getInstance().getConfig());

                long writeTimeConsuming = 0, tsRecordTimeConsuming = 0;
                // examine whether this TsFile should be merged
                for (Map.Entry<String, TsDeltaObject> tsFileEntry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    String tsFileDeltaObjectId = tsFileEntry.getKey();
                    if (unSeqFileMetaData.containsKey(tsFileDeltaObjectId) &&
                            unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).left <= tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).endTime) {
                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            if (unSeqFileMetaData.get(tsFileDeltaObjectId).containsKey(timeSeriesMetadata.getMeasurementUID())) {
                                TimeValuePairReader reader = ReaderCreator.createReaderForMerge(file.getPath(), unSeqFilePath,
                                        new Path(tsFileDeltaObjectId + "." + timeSeriesMetadata.getMeasurementUID()));
                                while (reader.hasNext()) {
                                    TimeValuePair tp = reader.next();

                                    // calc time consuming of constructing TsRecord
                                    long recordStartTime = System.currentTimeMillis();
                                    TSRecord record = constructTsRecord(tp, tsFileEntry.getKey(), timeSeriesMetadata.getMeasurementUID());
                                    long recordEndTime = System.currentTimeMillis();
                                    tsRecordTimeConsuming += (recordEndTime - recordStartTime);

                                    // calc time consuming of writing
                                    long writeStartTime = System.currentTimeMillis();
                                    fileWriter.write(record);
                                    long writeEndTime = System.currentTimeMillis();
                                    writeTimeConsuming += (writeEndTime - writeStartTime);
                                }
                                reader.close();
                            }
                        }
                    }
                }

                randomAccessFileReader.close();
                fileWriter.close();
                long oneFileMergeEndTime = System.currentTimeMillis();
                System.out.println(String.format("Current file merge time consuming : %dms, write consuming : %dms, " +
                                "construct record consuming: %dms, read consuming : %d",
                        (oneFileMergeEndTime - oneFileMergeStartTime), writeTimeConsuming, tsRecordTimeConsuming,
                        (oneFileMergeEndTime - oneFileMergeStartTime - writeTimeConsuming - tsRecordTimeConsuming)));
            }
        }

        long allFileMergeEndTime = System.currentTimeMillis();
        System.out.println(String.format("All file merge time consuming : %dms", allFileMergeEndTime - allFileMergeStartTime));
    }

    private static TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }

    private static TSEncoding getEncodingByDataType(TSDataType dataType) {
        switch (dataType) {
            case TEXT:
                return TSEncoding.PLAIN;
            default:
                return TSEncoding.RLE;
        }
    }

}
