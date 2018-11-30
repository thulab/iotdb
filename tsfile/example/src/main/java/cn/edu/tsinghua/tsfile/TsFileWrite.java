/**
 * There are two ways to construct a TsFile instance,they generate the same TsFile file.
 * The class use the second interface:
 * public void addMeasurement(MeasurementSchema measurementDescriptor) throws WriteProcessException
 */
package cn.edu.tsinghua.tsfile;

import java.io.File;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.*;

/**
 * An example of writing data to TsFile
 */
public class TsFileWrite {

    public static void main(String args[]) {
        try {
            String path = "test.tsfile";
            File f = new File(path);
            if (f.exists()) {
                f.delete();
            }
            TsFileWriter tsFileWriter = new TsFileWriter(f);

            // add measurements into file schema
            tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.RLE));
            tsFileWriter.addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT64, TSEncoding.RLE));

            for(long i = 1; i < 30L; i++) {
                // construct TSRecord
                TSRecord tsRecord = new TSRecord(i, "device_1");
                DataPoint dPoint1 = new LongDataPoint("sensor_1", i);
                DataPoint dPoint2 = new LongDataPoint("sensor_2", i);
                tsRecord.addTuple(dPoint1);
                tsRecord.addTuple(dPoint2);

                // write a TSRecord to TsFile
                tsFileWriter.write(tsRecord);
            }

            // close TsFile
            tsFileWriter.close();
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

}