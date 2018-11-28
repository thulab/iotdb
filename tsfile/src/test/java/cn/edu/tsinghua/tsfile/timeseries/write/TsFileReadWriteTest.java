package cn.edu.tsinghua.tsfile.timeseries.write;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DoubleDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;

public class TsFileReadWriteTest {
	private String path = "read_write_rle.tsfile";
	private File f;
	private TsFileWriter tsFileWriter;
	private final double delta = 0.0000001;

	
	@Before
	public void setUp() throws Exception {
		f = new File(path);
        if (f.exists()) {
            f.delete();
        }
        tsFileWriter = new TsFileWriter(f);
	}

	@After
	public void tearDown() throws Exception {
		f = new File(path);
        if (f.exists()) {
            f.delete();
        }
	}
	
	@Test
	public void intTest() throws IOException, WriteProcessException {
		int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new IntDataPoint("sensor_1", (int)i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();    
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals(i, r.getFields().get(new Path("device_1.sensor_1")).getInt(), delta);
            i++;
        }
        reader.close();
	}

	@Test
	public void longTest() throws IOException, WriteProcessException {
		int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new LongDataPoint("sensor_1", i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();    
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals(i, r.getFields().get(new Path("device_1.sensor_1")).getLong(), delta);
            i++;
        }
        reader.close();
	}

	@Test
	public void floatTest() throws IOException, WriteProcessException {
		int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new FloatDataPoint("sensor_1", (float)i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();    
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals((float)i, r.getFields().get(new Path("device_1.sensor_1")).getFloat(), delta);
            i++;
        }
        reader.close();
	}

	@Test
	public void doubleTest() throws IOException, WriteProcessException {
		int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.DOUBLE, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new DoubleDataPoint("sensor_1", (double)i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();    
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals((double)i, r.getFields().get(new Path("device_1.sensor_1")).getDouble(), delta);
            i++;
        }
        reader.close();
	}
	
}
