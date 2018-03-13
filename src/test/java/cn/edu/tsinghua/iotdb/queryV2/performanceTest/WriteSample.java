package cn.edu.tsinghua.iotdb.queryV2.performanceTest;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class WriteSample {

    private static final String tsFilePath = "/users/zhangjinrui/Desktop/readTest/out";
    private static final String fileFolderName = "\\D:\\data\\root.ln.wf632814.type4";

    public static void main(String args[]) throws WriteProcessException, IOException {
        long startTime = System.currentTimeMillis();

        FileSchema fileSchema = new FileSchema();

        Set<String> measurementSet = ReaderCreator.getAllMeasurements(fileFolderName);
        for (String measurement : measurementSet) {
            fileSchema.registerMeasurement(new MeasurementDescriptor("s_" + i, TSDataType.DOUBLE, TSEncoding.RLE));
        }
        File file = new File(tsFilePath);
        TsFileWriter fileWriter = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());
        TimeValuePairReader reader = ReaderCreator.createReaderForMerge(null, null, null);
        while (reader.hasNext()) {
            fileWriter.write(constructTsRecord(reader.next(), null, null));
        }
        fileWriter.close();
        reader.close();
        long endTime = System.currentTimeMillis();

        System.out.println("time consume : " + (endTime - startTime));
    }

    private static TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }
}
