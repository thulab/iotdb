package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;
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

import static cn.edu.tsinghua.iotdb.performance.CreatorUtils.*;
import static cn.edu.tsinghua.iotdb.performance.ReaderCreator.getTsFileMetadata;
import static cn.edu.tsinghua.iotdb.performance.ReaderCreator.getUnSeqFileMetaData;

public class WriteAnalysis {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteAnalysis.class);

    private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
    private static String mergeOutPutFolder;
    private static String fileFolderName;
    private static String unSeqFilePath;

    private static Map<String, Map<String, List<TimeSeriesChunkMetaData>>> unSeqFileMetaData;
    private static Map<String, Pair<Long, Long>> unSeqFileDeltaObjectTimeRangeMap;

    private static long max = -1;

    /**
     * @param args merge test fileFolderName
     */
    public static void main(String args[]) throws WriteProcessException, IOException {
        fileFolderName = args[0];
        mergeOutPutFolder = fileFolderName + "/merge/";
        unSeqFilePath = fileFolderName + "/" + unseqTsFilePathName;

        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        timer.scheduleAtFixedRate(new MaxMemory(), 1000, 10000, TimeUnit.MILLISECONDS);

        unSeqFileMetaData = getUnSeqFileMetaData(unSeqFilePath);
        WriteAnalysis writeAnalysis = new WriteAnalysis();
        writeAnalysis.initUnSeqFileStatistics();
        //writeAnalysis.unSeqTsFileReadPerformance();
        //writeAnalysis.tsFileReadPerformance();
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
            cnt++;
            // if (cnt == 10) break;
            if (!file.isDirectory() && !file.getName().endsWith(restoreFilePathName) &&
                    !file.getName().equals(unseqTsFilePathName) && !file.getName().endsWith("Store")
                    && !file.getName().equals("unseqTsFile")) {
                System.out.println(String.format("---- merge process begin, current merge file is %s.", file.getName()));

                long count = 0;
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
                TsFileIOWriter fileIOWriter = new TsFileIOWriter(new File(mergeOutPutFolder + file.getName()));

                //examine whether this TsFile should be merged
                for (Map.Entry<String, TsDeltaObject> tsFileEntry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    String deltaObjectId = tsFileEntry.getKey();
                    long tsFileDeltaObjectStartTime = tsFileMetaData.getDeltaObject(deltaObjectId).startTime;
                    long tsFileDeltaObjectEndTime = tsFileMetaData.getDeltaObject(deltaObjectId).endTime;
                    boolean isRowGroupHasData = false;

                    if (unSeqFileMetaData.containsKey(deltaObjectId) &&
                            unSeqFileDeltaObjectTimeRangeMap.get(deltaObjectId).left <= tsFileDeltaObjectStartTime
                            && unSeqFileDeltaObjectTimeRangeMap.get(deltaObjectId).right >= tsFileDeltaObjectStartTime) {

                        long recordCount = 0;
                        long startPos = 0;

                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            String measurementId = timeSeriesMetadata.getMeasurementUID();
                            TSDataType dataType = timeSeriesMetadata.getType();

                            if (unSeqFileMetaData.get(deltaObjectId).containsKey(measurementId)) {

                                TimeValuePairReader seriesReader = ReaderCreator.createReaderForMerge(file.getPath(), unSeqFilePath,
                                        new Path(deltaObjectId + "." + timeSeriesMetadata.getMeasurementUID()),
                                        tsFileDeltaObjectStartTime, tsFileDeltaObjectEndTime);

                                if (!seriesReader.hasNext()) {
                                    // LOGGER.debug("The time-series {} has no data");
                                    seriesReader.close();
                                } else {

                                    if (!isRowGroupHasData) {
                                        // start a new rowGroupMetadata
                                        isRowGroupHasData = true;
                                        fileIOWriter.startRowGroup(deltaObjectId);
                                        startPos = fileIOWriter.getPos();
                                    }

                                    // init the serieswWriteImpl
                                    MeasurementDescriptor desc = fileSchema.getMeasurementDescriptor(measurementId);
                                    IPageWriter pageWriter = new PageWriterImpl(desc);
                                    int pageSizeThreshold = TsFileConf.pageSizeInByte;
                                    SeriesWriterImpl seriesWriterImpl = new SeriesWriterImpl(deltaObjectId, desc, pageWriter,
                                            pageSizeThreshold);

                                    // write the series data
                                    recordCount += writeOneSeries(deltaObjectId, measurementId, seriesWriterImpl, dataType,
                                            (SeriesReader) seriesReader, new HashMap<>(), new HashMap<>());
                                    // flush the series data
                                    seriesWriterImpl.writeToFileWriter(fileIOWriter);

                                    seriesReader.close();
                                }
                            }
                        }

                        if (isRowGroupHasData) {
                            // end the new rowGroupMetadata
                            long memSize = fileIOWriter.getPos() - startPos;
                            fileIOWriter.endRowGroup(memSize, recordCount);
                        }
                    }
                }
                
                fileIOWriter.endFile(fileSchema);
                randomAccessFileReader.close();
                long oneFileMergeEndTime = System.currentTimeMillis();
                System.out.println(String.format("Current file merge time consuming : %sms, record number: %s," +
                                "original file size is %d, current file size is %d",
                        (oneFileMergeEndTime - oneFileMergeStartTime), count, file.length(), outPutFile.length()));
            }
        }

        long allFileMergeEndTime = System.currentTimeMillis();
        System.out.println(String.format("All file merge time consuming : %dms", allFileMergeEndTime - allFileMergeStartTime));
    }

    /**
     * Test the reading performance of TsFile.
     *
     * @throws IOException
     * @throws WriteProcessException
     */
    private void tsFileReadPerformance() throws IOException, WriteProcessException {
        Pair<Boolean, File[]> validFiles = getValidFiles(fileFolderName);
        if (!validFiles.left) {
            LOGGER.info("There exists error in given file folder");
            return;
        }
        File[] files = validFiles.right;

        long allFileMergeStartTime = System.currentTimeMillis();
        for (File file : files) {
            //Map<String, Map<String, Long>> seriesPoints = new HashMap<>();
            if (!file.isDirectory() && !file.getName().endsWith(restoreFilePathName) &&
                    !file.getName().equals(unseqTsFilePathName) && !file.getName().endsWith("Store")
                    && !file.getName().equals("unseqTsFile")) {
                System.out.println(String.format("---- merge process begin, current merge file is %s.", file.getName()));

                long oneFileReadStartTime = System.currentTimeMillis();
                long allRecordCount = 0;
                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);

                // construct file writer for the merge process
                FileSchema fileSchema = new FileSchema();
                File outPutFile = new File(mergeOutPutFolder + file.getName());
                for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                    fileSchema.registerMeasurement(new MeasurementDescriptor(timeSeriesMetadata.getMeasurementUID(), timeSeriesMetadata.getType(),
                            getEncodingByDataType(timeSeriesMetadata.getType())));
                }

                for (Map.Entry<String, TsDeltaObject> tsFileEntry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    String tsFileDeltaObjectId = tsFileEntry.getKey();
                    long tsFileDeltaObjectStartTime = tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).startTime;
                    long tsFileDeltaObjectEndTime = tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).endTime;

                    if (unSeqFileMetaData.containsKey(tsFileDeltaObjectId) &&
                            unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).left <= tsFileDeltaObjectStartTime
                            && unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).right >= tsFileDeltaObjectStartTime) {
                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            String measurementId = timeSeriesMetadata.getMeasurementUID();
                            Filter<?> filter = FilterFactory.and(TimeFilter.gtEq(tsFileDeltaObjectStartTime), TimeFilter.ltEq(tsFileDeltaObjectEndTime));
                            Path seriesPath = new Path(tsFileDeltaObjectId + "." + measurementId);
                            SeriesFilter<?> seriesFilter = new SeriesFilter<>(seriesPath, filter);
                            TimeValuePairReader reader = SeriesReaderFactory.getInstance().genTsFileSeriesReader(file.getPath(), seriesFilter);

                            //long tmpRecordCount = 0;
                            while (reader.hasNext()) {
                                TimeValuePair tp = reader.next();
                            }
//                            if (seriesPoints.containsKey(tsFileDeltaObjectId) && seriesPoints.get(tsFileDeltaObjectId).containsKey(measurementId)) {
//                                long originalCount = seriesPoints.get(tsFileDeltaObjectId).get(measurementId);
//                                seriesPoints.get(tsFileDeltaObjectId).put(measurementId, originalCount + tmpRecordCount);
//                            } else {
//                                if (!seriesPoints.containsKey(tsFileDeltaObjectId)) {
//                                    seriesPoints.put(tsFileDeltaObjectId, new HashMap<>());
//                                }
//                                seriesPoints.get(tsFileDeltaObjectId).put(measurementId, tmpRecordCount);
//                            }
                            //System.out.println(String.format("%s.%s: %d", tsFileDeltaObjectId, timeSeriesMetadata.getMeasurementUID(), tmpRecordCount));
                            //allRecordCount += tmpRecordCount;
                            reader.close();
                        }
                    }
                }
                //seriesPoints.clear();

                long oneFileReadEndTime = System.currentTimeMillis();
                System.out.println(String.format("current file read time: %sms, record number : %s", oneFileReadEndTime - oneFileReadStartTime, allRecordCount));

            }
        }

        long allFileMergeEndTime = System.currentTimeMillis();
        System.out.println(String.format("All file merge time consuming : %dms", allFileMergeEndTime - allFileMergeStartTime));
    }

    /**
     * Test the reading performance of UnSeqTsFile.
     *
     * @throws IOException
     * @throws WriteProcessException
     */
    private void unSeqTsFileReadPerformance() throws IOException {

        Pair<Boolean, File[]> validFiles = getValidFiles(fileFolderName);
        if (!validFiles.left) {
            LOGGER.info("There exists error in given file folder");
            return;
        }
        File[] files = validFiles.right;

        int cnt = 0;
        for (File file : files) {
            cnt++;
            // if (cnt == 10) break;
            if (!file.isDirectory() && !file.getName().endsWith(restoreFilePathName) &&
                    !file.getName().equals(unseqTsFilePathName) && !file.getName().endsWith("Store")
                    && !file.getName().equals("unseqTsFile")) {
                System.out.println(String.format("---- merge process begin, current merge file is %s.", file.getName()));

                long count = 0;
                long oneFileMergeStartTime = System.currentTimeMillis();
                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);

                //examine whether this TsFile should be merged
                for (Map.Entry<String, TsDeltaObject> tsFileEntry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    String tsFileDeltaObjectId = tsFileEntry.getKey();
                    long tsFileDeltaObjectStartTime = tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).startTime;
                    long tsFileDeltaObjectEndTime = tsFileMetaData.getDeltaObject(tsFileDeltaObjectId).endTime;
                    if (unSeqFileMetaData.containsKey(tsFileDeltaObjectId) &&
                            unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).left <= tsFileDeltaObjectStartTime
                            && unSeqFileDeltaObjectTimeRangeMap.get(tsFileDeltaObjectId).right >= tsFileDeltaObjectStartTime) {
                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            if (unSeqFileMetaData.get(tsFileDeltaObjectId).containsKey(timeSeriesMetadata.getMeasurementUID())) {
                                String measurementId = timeSeriesMetadata.getMeasurementUID();
                                Filter<?> filter = FilterFactory.and(TimeFilter.gtEq(tsFileDeltaObjectStartTime), TimeFilter.ltEq(tsFileDeltaObjectEndTime));
                                Path seriesPath = new Path(tsFileDeltaObjectId + "." + measurementId);
                                SeriesFilter<?> seriesFilter = new SeriesFilter<>(seriesPath, filter);
                                TimeValuePairReader tsFileReader = SeriesReaderFactory.getInstance().genTsFileSeriesReader(file.getPath(), seriesFilter);
                                while (tsFileReader.hasNext()) {
                                    TimeValuePair tp = tsFileReader.next();
                                    count ++;
                                }
                                tsFileReader.close();


                                TimeValuePairReader overflowInsertReader = ReaderCreator.createReaderOnlyForOverflowInsert(unSeqFilePath,
                                        new Path(tsFileDeltaObjectId + "." + timeSeriesMetadata.getMeasurementUID()),
                                        tsFileDeltaObjectStartTime, tsFileDeltaObjectEndTime);
                                while (overflowInsertReader.hasNext()) {
                                    TimeValuePair tp = overflowInsertReader.next();
                                    count ++;
                                }
                                overflowInsertReader.close();
                            }
                        }
                    }
                }

                randomAccessFileReader.close();
                long oneFileMergeEndTime = System.currentTimeMillis();
                System.out.println(String.format("Current file merge time consuming : %sms, record number: %s," +
                                "original file size is %d",
                        (oneFileMergeEndTime - oneFileMergeStartTime), count, file.length()));
            }
        }
    }

    private int writeOneSeries(String deltaObjectId, String measurement, SeriesWriterImpl seriesWriterImpl,
                               TSDataType dataType, SeriesReader seriesReader, Map<String, Long> startTimeMap,
                               Map<String, Long> endTimeMap) throws IOException {
        int count = 0;
        if (!seriesReader.hasNext())
            return 0;
        TimeValuePair timeValuePair = seriesReader.next();
        long startTime = -1;
        long endTime = -1;
        switch (dataType) {
            case BOOLEAN:
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
                count++;
                startTime = endTime = timeValuePair.getTimestamp();
                if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                    startTimeMap.put(deltaObjectId, startTime);
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                while (seriesReader.hasNext()) {
                    count++;
                    timeValuePair = seriesReader.next();
                    endTime = timeValuePair.getTimestamp();
                    seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                break;
            case INT32:
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
                count++;
                startTime = endTime = timeValuePair.getTimestamp();
                if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                    startTimeMap.put(deltaObjectId, startTime);
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                while (seriesReader.hasNext()) {
                    count++;
                    timeValuePair = seriesReader.next();
                    endTime = timeValuePair.getTimestamp();
                    seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                break;
            case INT64:
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
                count++;
                startTime = endTime = timeValuePair.getTimestamp();
                if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                    startTimeMap.put(deltaObjectId, startTime);
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                while (seriesReader.hasNext()) {
                    count++;
                    timeValuePair = seriesReader.next();
                    endTime = timeValuePair.getTimestamp();
                    seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                break;
            case FLOAT:
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
                count++;
                startTime = endTime = timeValuePair.getTimestamp();
                if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                    startTimeMap.put(deltaObjectId, startTime);
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                while (seriesReader.hasNext()) {
                    count++;
                    timeValuePair = seriesReader.next();
                    endTime = timeValuePair.getTimestamp();
                    seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                break;
            case DOUBLE:
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
                count++;
                startTime = endTime = timeValuePair.getTimestamp();
                if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                    startTimeMap.put(deltaObjectId, startTime);
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                while (seriesReader.hasNext()) {
                    count++;
                    timeValuePair = seriesReader.next();
                    endTime = timeValuePair.getTimestamp();
                    seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                break;
            case TEXT:
                seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
                count++;
                startTime = endTime = timeValuePair.getTimestamp();
                if (!startTimeMap.containsKey(deltaObjectId) || startTimeMap.get(deltaObjectId) > startTime) {
                    startTimeMap.put(deltaObjectId, startTime);
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                while (seriesReader.hasNext()) {
                    count++;
                    timeValuePair = seriesReader.next();
                    endTime = timeValuePair.getTimestamp();
                    seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
                }
                if (!endTimeMap.containsKey(deltaObjectId) || endTimeMap.get(deltaObjectId) < endTime) {
                    endTimeMap.put(deltaObjectId, endTime);
                }
                break;
            default:
                LOGGER.error("Not support data type: {}", dataType);
                break;
        }
        return count;
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
            //System.out.println("current max memory: " + max);
        }
    }
}
