package cn.edu.tsinghua.iotdb.queryV2.performanceTest;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.CreatorUtils.getValidFiles;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.CreatorUtils.restoreFilePathName;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.CreatorUtils.unseqTsFilePathName;
import static cn.edu.tsinghua.iotdb.queryV2.performanceTest.ReaderCreator.getTsFileMetadata;

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class WriteSample {

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteSample.class);

    private static final String tsFileOutPutPath = "/users/zhangjinrui/Desktop/readTest/out";
    private static final String fileFolderName = "\\D:\\data\\root.ln.wf632814.type4\\";


    public static void main(String args[]) throws WriteProcessException, IOException {
        executeMerge();
//        long startTime = System.currentTimeMillis();
//
//        FileSchema fileSchema = new FileSchema();
//
//        Map<String, TSDataType> measurementDataTypeMap = ReaderCreator.getAllMeasurements(fileFolderName);
//        for (Map.Entry<String, TSDataType> entry : measurementDataTypeMap.entrySet()) {
//            fileSchema.registerMeasurement(new MeasurementDescriptor(entry.getKey(), entry.getValue(), getEncodingByDataType(entry.getValue())));
//        }
//
//        File file = new File(tsFileOutPutPath);
//        TsFileWriter fileWriter = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());
//
//        Set<Path> pathSet = ReaderCreator.getAllPaths(fileFolderName);
//        for (Path path : pathSet) {
//            TimeValuePairReader reader = ReaderCreator.createReaderForMerge(null, null, null);
//            while (reader.hasNext()) {
//                fileWriter.write(constructTsRecord(reader.next(), path.getDeltaObjectToString(), path.getMeasurementToString()));
//            }
//            // fileWriter.close();
//            reader.close();
//        }
//        fileWriter.close();
//        long endTime = System.currentTimeMillis();
//
//        System.out.println("time consume : " + (endTime - startTime));
    }

    private static void executeMerge() throws IOException, WriteProcessException {
        Pair<Boolean, File[]> validFiles = getValidFiles(fileFolderName);
        if (!validFiles.left)
            LOGGER.info("There exists error in given file folder");
        File[] files = validFiles.right;

        long start = System.currentTimeMillis();
        long executeTime = 0;

        // construct the file reader of unseqTsFile, get the changed time range
        String unSeqFilePath = fileFolderName + "\\" + unseqTsFilePathName;
        ITsRandomAccessFileReader unSeqFileReader = new TsRandomAccessLocalFileReader(unSeqFilePath);
        TsFileMetaData unSeqFileMetaData = getTsFileMetadata(unSeqFileReader);
        //Map<String, TsDeltaObject> unSeqFileTimeMap = unSeqFileMetaData.getDeltaObjectMap();

        for (File file : files) {
            if (!file.getName().endsWith(fileFolderName + "\\" + restoreFilePathName) && !file.getName().equals(fileFolderName + "\\" + unseqTsFilePathName)) {
                System.out.println("merge process begin : " + file.getName());

                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getPath());
                TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);

                // construct file writer for the merge process
                FileSchema fileSchema = new FileSchema();
                File outPutFile = new File(file.getName() + ".merge");
                // register measurements
                for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                    fileSchema.registerMeasurement(new MeasurementDescriptor(timeSeriesMetadata.getMeasurementUID(), timeSeriesMetadata.getType(),
                            getEncodingByDataType(timeSeriesMetadata.getType())));
                }
                TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema, TSFileDescriptor.getInstance().getConfig());

                // examine whether this TsFile should be merged
                for (Map.Entry<String, TsDeltaObject> entry : tsFileMetaData.getDeltaObjectMap().entrySet()) {
                    if (unSeqFileMetaData.containsDeltaObject(entry.getKey())
                            && unSeqFileMetaData.getDeltaObject(entry.getKey()).endTime <= tsFileMetaData.getDeltaObject(entry.getKey()).startTime) {
                        for (TimeSeriesMetadata timeSeriesMetadata : tsFileMetaData.getTimeSeriesList()) {
                            TimeValuePairReader reader = ReaderCreator.createReaderForMerge(file.getPath(), unSeqFilePath,
                                    new Path(entry.getKey() + "." + timeSeriesMetadata.getMeasurementUID()));
                            while (reader.hasNext()) {
                                fileWriter.write(constructTsRecord(reader.next(), entry.getKey(), timeSeriesMetadata.getMeasurementUID()));
                            }
                            reader.close();
                        }
                    }
                }

                randomAccessFileReader.close();
                fileWriter.close();
            }
        }

        long end = System.currentTimeMillis();

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
