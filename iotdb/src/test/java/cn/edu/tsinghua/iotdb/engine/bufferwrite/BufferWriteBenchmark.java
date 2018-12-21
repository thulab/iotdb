package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BufferWriteBenchmark {
    private static int numOfDevice = 100;
    private static String[] deviceIds = new String[numOfDevice];
    static {
        for(int i = 0;i<numOfDevice;i++){
            deviceIds[i] = String.valueOf("d"+i);
        }
    }
    private static int numOfMeasurement = 100;
    private static String[] measurementIds  = new String[numOfMeasurement];
    private static FileSchema fileSchema = new FileSchema();
    private static TSDataType tsDataType = TSDataType.INT64;
    private static int numOfPoint = 1000;
    static {
        for(int i = 0;i< numOfMeasurement;i++){
            measurementIds[i] = String.valueOf("m"+i);
            MeasurementDescriptor measurementDescriptor = new MeasurementDescriptor("m"+i,
                    tsDataType, TSEncoding.PLAIN);
            assert measurementDescriptor.getCompressor()!=null;
            fileSchema.registerMeasurement(measurementDescriptor);

        }
    }

    private static void before() throws IOException {
        FileUtils.deleteDirectory(new File("BufferBenchmark"));
    }

    private static void after() throws IOException {
        FileUtils.deleteDirectory(new File("BufferBenchmark"));
    }

    public static void main(String[] args) throws BufferWriteProcessorException, IOException {
        before();
        Map<String,Object> parameters = new HashMap<>();
        parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
            }
        });
        parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
            }
        });
        parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, new Action() {
            @Override
            public void act() throws Exception {
                System.out.println(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
            }
        });

        BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(
                "BufferBenchmark","bench","benchFile",parameters,fileSchema);

        long startTime = System.currentTimeMillis();
        for(int i = 0;i<numOfPoint;i++){
            for(int j = 0;j<numOfDevice;j++){
                TSRecord tsRecord = getRecord(deviceIds[j]);
                bufferWriteProcessor.write(tsRecord);
            }
        }
        long endTime = System.currentTimeMillis();
        bufferWriteProcessor.close();
        System.out.println(String.format("Num of time series: %d, " +
                "Num of points for each time series: %d, " +
                "The total time: %d ms. ",numOfMeasurement*numOfDevice,
                numOfPoint,endTime-startTime));

        after();
    }

    private static TSRecord getRecord(String deviceId){
        long time = System.nanoTime();
        long value = System.nanoTime();
        TSRecord tsRecord = new TSRecord(time,deviceId);
        for(String measurement: measurementIds)
            tsRecord.addTuple(new LongDataPoint(measurement,value));
        return tsRecord;
    }
}
