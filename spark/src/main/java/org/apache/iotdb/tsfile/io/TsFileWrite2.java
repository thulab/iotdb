package org.apache.iotdb.tsfile.io;

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class TsFileWrite2 {

  public static void main(String args[]) {
    try {
      String path = "spark/src/test/resources/test2.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }
      TsFileWriter tsFileWriter = new TsFileWriter(f);

      int floatCount = 1024 * 1024 * 13 + 1023;
      // add measurements into file schema
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));
      for (long i = 1; i < floatCount; i++) {
        // construct TSRecord
        TSRecord tsRecord = new TSRecord(i, "device_1");
        DataPoint dPoint1 = new IntDataPoint("sensor_1", (int) i);
        tsRecord.addTuple(dPoint1);
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

