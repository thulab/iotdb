package cn.edu.tsinghua.iotdb.performance;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.engine.memtable.PrimitiveMemTable;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class MergeWritePerfCompaeTest {

	private static final String outPutFilePath = "output";
	private static long memThreshold = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
	private static AtomicLong memSize = new AtomicLong();
	private static IMemTable workMemTable;
	private static TsFileIOWriter writer;

	public static void main(String[] args) throws WriteProcessException, IOException {
		writeMultiSeriesTest();
		//writeSingleSeriesTest();
	}

	private static void writeMultiSeriesTest() throws WriteProcessException, IOException {
		FileSchema fileSchema = new FileSchema();
		File outPutFile = new File(outPutFilePath);
		outPutFile.delete();
		for (int i = 1; i <= 100; i++)
			fileSchema.registerMeasurement(new MeasurementDescriptor("s" + i, TSDataType.FLOAT, TSEncoding.RLE));
		// TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema,
		// TSFileDescriptor.getInstance().getConfig());
		writer = new TsFileIOWriter(outPutFile);
		memSize.set(0);
		workMemTable = new PrimitiveMemTable();
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= 500; i++) {
			for (int j = 1; j <= 100; j++) {
				for (long time = 1; time <= 2257; time++) {
					TSRecord record = new TSRecord(time, "d" + i);
					record.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "s" + j, String.valueOf(4.0)));
					workMemTable.write(record.deltaObjectId, record.dataPointList.get(0).getMeasurementId(),
							record.dataPointList.get(0).getType(), record.time,
							record.dataPointList.get(0).getValue().toString());
				
					long mem = memSize.addAndGet(MemUtils.getRecordSize(record));
					//System.out.println(MemUtils.bytesCntToStr(mem));
					if (mem > memThreshold) {
						MemTableFlushUtil.flushMemTable(fileSchema, writer, workMemTable);
						memSize.set(0);
					}
				}
			}
		}
		writer.endFile(fileSchema);
		long endTime = System.currentTimeMillis();
		System.out.println(String.format("write multi series time cost %dms", endTime - startTime));
	}

	private static void writeSingleSeriesTest() throws WriteProcessException, IOException {
		FileSchema fileSchema = new FileSchema();
		File outPutFile = new File(outPutFilePath);
		fileSchema.registerMeasurement(new MeasurementDescriptor("s0", TSDataType.FLOAT, TSEncoding.RLE));
		// TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema,
		// TSFileDescriptor.getInstance().getConfig());
		writer = new TsFileIOWriter(outPutFile);
		memSize.set(0);
		workMemTable = new PrimitiveMemTable();
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= 112850000; i++) {
			TSRecord record = new TSRecord(i, "d0");
			record.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "s0", String.valueOf(4.0)));
			workMemTable.write(record.deltaObjectId, record.dataPointList.get(0).getMeasurementId(),
					record.dataPointList.get(0).getType(), record.time,
					record.dataPointList.get(0).getValue().toString());
			long mem = memSize.addAndGet(MemUtils.getRecordSize(record));
			System.out.println(MemUtils.bytesCntToStr(mem));
			if (mem > memThreshold) {
				MemTableFlushUtil.flushMemTable(fileSchema, writer, workMemTable);
				memSize.set(0);
			}
		}
		writer.endFile(fileSchema);
		long endTime = System.currentTimeMillis();
		System.out.println(String.format("write single series time cost %dms", endTime - startTime));
	}

}
