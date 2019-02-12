package org.apache.iotdb.db;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * An example of writing data to TsFile
 */
public class TsfileWriteTest {

  public static void main(String args[]) {
    try {
      String path = "test2.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }
      TsFileWriter tsFileWriter = new TsFileWriter(f);

      // add measurements into file schema
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
      tsFileWriter
          .addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

      // construct TSRecord
      TSRecord tsRecord = new TSRecord(1, "device_1");
      DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
      DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
      DataPoint dPoint3;
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);

      // write a TSRecord to TsFile
      tsFileWriter.write(tsRecord);

      tsRecord = new TSRecord(2, "device_1");
      dPoint2 = new IntDataPoint("sensor_2", 20);
      dPoint3 = new IntDataPoint("sensor_3", 50);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
      tsFileWriter.write(tsRecord);

      tsRecord = new TSRecord(3, "device_2");
      dPoint1 = new FloatDataPoint("sensor_1", 1.4f);
      dPoint2 = new IntDataPoint("sensor_2", 21);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsFileWriter.write(tsRecord);

      tsRecord = new TSRecord(4, "device_3");
      dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
      dPoint2 = new IntDataPoint("sensor_2", 20);
      dPoint3 = new IntDataPoint("sensor_3", 51);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
      tsFileWriter.write(tsRecord);

      tsRecord = new TSRecord(6, "device_4");
      dPoint1 = new FloatDataPoint("sensor_1", 7.2f);
      dPoint2 = new IntDataPoint("sensor_2", 10);
      dPoint3 = new IntDataPoint("sensor_3", 11);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
      tsFileWriter.write(tsRecord);

      tsRecord = new TSRecord(7, "device_4");
      dPoint1 = new FloatDataPoint("sensor_1", 6.2f);
      dPoint2 = new IntDataPoint("sensor_2", 20);
      dPoint3 = new IntDataPoint("sensor_3", 21);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
      tsFileWriter.write(tsRecord);

      tsRecord = new TSRecord(8, "device_4");
      dPoint1 = new FloatDataPoint("sensor_1", 9.2f);
      dPoint2 = new IntDataPoint("sensor_2", 30);
      dPoint3 = new IntDataPoint("sensor_3", 31);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      tsRecord.addTuple(dPoint3);
      tsFileWriter.write(tsRecord);

      // close TsFile
      tsFileWriter.close();

      TsFileSequenceReader reader = new TsFileSequenceReader(path);
      ReadOnlyTsFile roTsFile = new ReadOnlyTsFile(reader);
      TsFileMetaData tsFileMetaData = reader.readFileMetadata();

//      System.out.println("");
      List<Path> pathList = new ArrayList<>();
      pathList.add(new Path("device_1.sensor_1"));
      pathList.add(new Path("device_1.sensor_2"));
      IExpression valFilter = new SingleSeriesExpression(new Path("device_1.sensor_2"), ValueFilter.gt(10));
      IExpression tFilter = BinaryExpression
          .and(new GlobalTimeExpression(TimeFilter.gtEq(0L)),
              new GlobalTimeExpression(TimeFilter.lt(7L)));
      IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
      QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
      QueryDataSet dataSet = roTsFile.query(queryExpression);

      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        System.out.println(rowRecord);
        cnt++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
