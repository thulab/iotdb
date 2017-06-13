package cn.edu.thu.tsfile.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONObject;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.FileFormat.TsFile;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;

public class TsFileTestHelper {
	
	public static void deleteTsFile(String filePath){
		File file = new File(filePath);
		file.delete();
	}

	public static void writeTsFile(String filePath) {
		
		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		conf.pageSize=100;
		conf.rowGroupSize = 2000;
		conf.pageCheckSizeThreshold = 1;
		conf.defaultMaxStringLength = 2;

		File file = new File(filePath);

		if (file.exists())
			file.delete();

		JSONObject jsonSchema = getJsonSchema();
		
		try {
			TSRandomAccessFileWriter output = new RandomAccessOutputStream(new File(filePath));
			TsFile tsFile = new TsFile(output, jsonSchema);
			String line = "";
			for(int i = 1;i<100;i++){
				line = "root.car.d1,"+i+",s1,1,s2,1,s3,0.1,s4,0.1";
				tsFile.writeLine(line);
			}
			tsFile.writeLine("root.car.d2,5, s1, 5, s2, 50, s3, 200.5, s4, 0.5");
			tsFile.writeLine("root.car.d2,6, s1, 6, s2, 60, s3, 200.6, s4, 0.6");
			tsFile.writeLine("root.car.d2,7, s1, 7, s2, 70, s3, 200.7, s4, 0.7");
			tsFile.writeLine("root.car.d2,8, s1, 8, s2, 80, s3, 200.8, s4, 0.8");
			tsFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		} catch (WriteProcessException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	private static JSONObject getJsonSchema() {
		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		JSONObject s1 = new JSONObject();
		s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
		s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
		s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONObject s2 = new JSONObject();
		s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
		s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
		s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONObject s3 = new JSONObject();
		s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
		s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
		s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONObject s4 = new JSONObject();
		s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
		s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
		s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONArray measureGroup = new JSONArray();
		measureGroup.put(s1);
		measureGroup.put(s2);
		measureGroup.put(s3);
		measureGroup.put(s4);

		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);
		return jsonSchema;
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		String filePath = "tsfile";
		File file = new File(filePath);
		System.out.println(file.exists());
		file.delete();
		writeTsFile(filePath);
		TsFile tsFile = new TsFile(new LocalFileInput(filePath));
		System.out.println(tsFile.getAllColumns());
		System.out.println(tsFile.getAllDeltaObject());
		System.out.println(tsFile.getAllSeries());
		System.out.println(tsFile.getDeltaObjectRowGroupCount());
		System.out.println(tsFile.getDeltaObjectTypes());
		System.out.println(tsFile.getRowGroupPosList());
		
	}
}
