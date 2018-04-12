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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.getValidFiles;
import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.restoreFilePathName;
import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.unseqTsFilePathName;
import static cn.edu.tsinghua.iotdb.performance.ReaderCreator.getTsFileMetadata;
import static cn.edu.tsinghua.iotdb.performance.ReaderCreator.getUnSeqFileMetaData;

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class WriteAnalysis {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteAnalysis.class);

    private static String mergeOutPutFolder;
    private static String fileFolderName;
    private static String unSeqFilePath;


    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> unSeqFileMetaData;
    private static Map<String, Pair<Long, Long>> unSeqFileDeltaObjectTimeRangeMap;

    private static long max = -1;

    public static void main(String args[]) throws WriteProcessException, IOException, InterruptedException {
        fileFolderName = args[0];
        mergeOutPutFolder = fileFolderName + "/merge/";
        unSeqFilePath = fileFolderName + "/" + unseqTsFilePathName;

        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        timer.scheduleAtFixedRate(new MaxMemory(), 1000, 3000, TimeUnit.MILLISECONDS);

        unSeqFileMetaData = getUnSeqFileMetaData(unSeqFilePath);
        WriteAnalysis writeAnalysis = new WriteAnalysis();
        writeAnalysis.initUnSeqFileStatistics();

        //Thread.sleep(10000);

        writeAnalysis.executeMerge();
        System.out.println("max memory usage:" + max);
        timer.shutdown();
    }

    private void initUnSeqFileStatistics() {
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

    private void executeMerge() throws IOException, WriteProcessException {
        Pair<Boolean, File[]> validFiles = getValidFiles(fileFolderName);
        if (!validFiles.left) {
            LOGGER.info("There exists error in given file folder");
            return;
        }
        File[] files = validFiles.right;

        long allFileMergeStartTime = System.currentTimeMillis();
        int cnt = 0;
        for (File file : files) {
            cnt ++;
//            if (cnt == 10) {
//                break;
//            }
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
                int readStreamLoadTime = 0, hasNextCalculationTime = 0;
                // examine whether this TsFile should be merged
                for (Map.Entry<String, TsDeltaObject> tsFileEntry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    String tsFileDeltaObjectId = tsFileEntry.getKey();
                    long tsFileDeltaObjectStartTime = tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).startTime;
                    long tsFileDeltaObjectEndTime = tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).endTime;
                    if (unSeqFileMetaData.containsKey(tsFileDeltaObjectId) &&
                            unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).left <= tsFileDeltaObjectStartTime
                            && unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).right >= tsFileDeltaObjectStartTime) {
                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            if (unSeqFileMetaData.get(tsFileDeltaObjectId).containsKey(timeSeriesMetadata.getMeasurementUID())) {

                                // calc file stream load time
                                long streamLoadTimeStart = System.currentTimeMillis();
                                TimeValuePairReader reader = ReaderCreator.createReaderForMerge(file.getPath(), unSeqFilePath,
                                        new Path(tsFileDeltaObjectId + "." + timeSeriesMetadata.getMeasurementUID()),
                                        tsFileDeltaObjectStartTime, tsFileDeltaObjectEndTime);
                                long streamLoadTimeEnd = System.currentTimeMillis();
                                readStreamLoadTime += (streamLoadTimeEnd - streamLoadTimeStart);

                                // calc hasnext method executing time
                                long hasNextCalcStartTime = System.currentTimeMillis();
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

                                long hasNextCalcEndTime = System.currentTimeMillis();
                                hasNextCalculationTime += (hasNextCalcEndTime - hasNextCalcStartTime);
                                reader.close();
                            }
                        }
                    }
                }

                randomAccessFileReader.close();
                fileWriter.close();
                long oneFileMergeEndTime = System.currentTimeMillis();
                System.out.println(String.format("Current file merge time consuming : %dms," +
                                "write consuming : %dms, construct record consuming: %dms," +
                                "stream load consuming : %dms, calc next consuming : %dms",
                        (oneFileMergeEndTime - oneFileMergeStartTime),
                        writeTimeConsuming, tsRecordTimeConsuming,
                        readStreamLoadTime, hasNextCalculationTime - writeTimeConsuming - tsRecordTimeConsuming));
            }
        }

        long allFileMergeEndTime = System.currentTimeMillis();
        System.out.println(String.format("All file merge time consuming : %dms", allFileMergeEndTime - allFileMergeStartTime));
    }

    private TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }

    private TSEncoding getEncodingByDataType(TSDataType dataType) {
        switch (dataType) {
            case TEXT:
                return TSEncoding.PLAIN;
            default:
                return TSEncoding.RLE;
        }
    }

    public static class MaxMemory implements Runnable {
        @Override
        public void run() {
            max = Math.max(max, Runtime.getRuntime().totalMemory());
        }
    }
}
