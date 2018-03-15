package cn.edu.tsinghua.iotdb.queryV2.performanceTest;

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
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.CreatorUtils.getValidFiles;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.CreatorUtils.restoreFilePathName;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.CreatorUtils.unseqTsFilePathName;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.ReaderCreator.getTsFileMetadata;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.ReaderCreator.getUnSeqFileMetaData;

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class WriteSample {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteSample.class);

    private static final String tsFileOutPutPath = "/users/zhangjinrui/Desktop/readTest/out";
    private static final String fileFolderName = "D:\\data\\root.ln.wf632814.type4";
    private static final String unSeqFilePath = fileFolderName + "\\" + unseqTsFilePathName;

    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> unSeqFileMetaData;
    private static Map<String, Pair<Long, Long>> unSeqFileDeltaObjectTimeRangeMap;

    public static void main(String args[]) throws WriteProcessException, IOException {

        unSeqFileMetaData = getUnSeqFileMetaData(unSeqFilePath);

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

        executeMerge();
    }

    private static void executeMerge() throws IOException, WriteProcessException {
        Pair<Boolean, File[]> validFiles = getValidFiles(fileFolderName);
        if (!validFiles.left)
            LOGGER.info("There exists error in given file folder");
        File[] files = validFiles.right;

        long startTime = System.currentTimeMillis();
        long executeTime = 0;

        // construct the file reader of unseqTsFile, get the changed time range

        for (File file : files) {
            if (!file.getName().endsWith(restoreFilePathName) && !file.getName().equals(unseqTsFilePathName)) {
                System.out.println(String.format("---- merge process begin, current merge file is %s.", file.getName()));

                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);

                // construct file writer for the merge process
                FileSchema fileSchema = new FileSchema();
                File outPutFile = new File(file.getParent() + "\\merge\\" + file.getName());
                // register measurements
                for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                    fileSchema.registerMeasurement(new MeasurementDescriptor(timeSeriesMetadata.getMeasurementUID(), timeSeriesMetadata.getType(),
                            getEncodingByDataType(timeSeriesMetadata.getType())));
                }
                TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema, TSFileDescriptor.getInstance().getConfig());

//                for (Map.Entry<String, Map<String, List<TimeSeriesChunkMetaData>>> entry : unSeqFileMetaData.entrySet()) {
//                    String unSeqDeltaObjectId = entry.getKey();
//                    if (tsFileMetaData.containsDeltaObject(unSeqDeltaObjectId)) {
//                        for (Map.Entry<String, List<TimeSeriesChunkMetaData>> unSeqTsEntry : entry.getValue().entrySet()) {
//                            String unSeqMeasurementId = unSeqTsEntry.getKey();
//                            TimeValuePairReader reader = ReaderCreator.createReaderForMerge(file.getPath(), unSeqFilePath, new Path(unSeqDeltaObjectId + "." + unSeqMeasurementId));
//                            while (reader.hasNext()) {
//                                fileWriter.write(constructTsRecord(reader.next(), unSeqDeltaObjectId, unSeqMeasurementId));
//                            }
//                            reader.close();
//
////                            for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : unSeqTsEntry.getValue()) {
////                                if (timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime() <= tsFileMetaData.getDeltaObject(unSeqDeltaObject).endTime) {
////                                }
////                            }
//                        }
//                    }
//                }

                // examine whether this TsFile should be merged
                for (Map.Entry<String, TsDeltaObject> tsFileEntry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    String tsFileDeltaObjectId = tsFileEntry.getKey();
                    if (unSeqFileMetaData.containsKey(tsFileDeltaObjectId) &&
                            unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).left <= tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).endTime) {
                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            TimeValuePairReader reader = ReaderCreator.createReaderForMerge(file.getPath(), unSeqFilePath, new Path(tsFileDeltaObjectId + "." + timeSeriesMetadata.getMeasurementUID()));
                            while (reader.hasNext()) {
                                fileWriter.write(constructTsRecord(reader.next(), tsFileEntry.getKey(), timeSeriesMetadata.getMeasurementUID()));
                            }
                            reader.close();
                        }
                    }
                }

                randomAccessFileReader.close();
                fileWriter.close();
            }
            //break;
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("time usage : %d", endTime - startTime));
        System.out.println(startTime + "." + endTime);
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
