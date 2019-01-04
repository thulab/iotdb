package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;

public class OverflowTestUtils {
	public static String deltaObjectId1 = "d1";
	public static String deltaObjectId2 = "d2";
	public static String measurementId1 = "s1";
	public static String measurementId2 = "s2";
	public static TSDataType dataType1 = TSDataType.INT32;
	public static TSDataType dataType2 = TSDataType.FLOAT;
	private static FileSchema fileSchema = new FileSchema();
	static {
		fileSchema.registerMeasurement(new MeasurementSchema(measurementId1, dataType1, TSEncoding.PLAIN));
		fileSchema.registerMeasurement(new MeasurementSchema(measurementId2, dataType2, TSEncoding.PLAIN));
	}

	public static FileSchema getFileSchema() {
		return fileSchema;
	}

	public static void produceInsertData(OverflowSupport support) {
		support.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(1), 1));
		support.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(3), 3));
		support.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(2), 2));

		support.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 1));
		support.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 2));
		support.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(10.5f), 2));
	}

	private static TSRecord getData(String d, String m, TSDataType type, String value, long time) {
		TSRecord record = new TSRecord(time, d);
		record.addTuple(DataPoint.getDataPoint(type, m, value));
		return record;
	}

	public static void produceInsertData(OverflowProcessor processor) throws IOException {

		processor.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(1), 1));
		processor.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(3), 3));
		processor.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(2), 2));

		processor.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 1));
		processor.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 2));
		processor.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(10.5f), 2));
	}

}
