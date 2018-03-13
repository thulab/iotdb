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

/**
 * Created by zhangjinrui on 2018/3/13.
 */
public class WriteSample {
    public static void main(String args[]) throws WriteProcessException, IOException {
        long startTime = System.currentTimeMillis();
        FileSchema fileSchema = new FileSchema();
        for (int i = 0; i < 60; i++) {
            fileSchema.registerMeasurement(new MeasurementDescriptor("s_" + i, TSDataType.DOUBLE, TSEncoding.RLE));
        }
        File file = new File("/users/zhangjinrui/Desktop/readTest/out");
        TsFileWriter fileWriter = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());
        TimeValuePairReader reader = ReaderCreater.createReaerForMerge(null, null, null);
        while (reader.hasNext()) {
            fileWriter.write(constructTsRecord(reader.next(), null, null));
        }
        fileWriter.close();
        reader.close();
    }

    private static TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }
}
