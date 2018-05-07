package cn.edu.tsinghua.iotdb.performance;

import java.io.File;
import java.io.IOException;
import java.util.Random;
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
	private static Random random = new Random(System.currentTimeMillis());
	private static IMemTable workMemTable;
	private static TsFileIOWriter writer;

	public static void main(String[] args) throws WriteProcessException, IOException {
		writeMultiSeriesTest();
//		writeSingleSeriesTest();
	}

	private static void writeMultiSeriesTest() throws WriteProcessException, IOException {
		FileSchema fileSchema = new FileSchema();
		File outPutFile = new File(outPutFilePath);
		outPutFile.delete();
		for (int i = 1; i <= 100; i++)
			fileSchema.registerMeasurement(new MeasurementDescriptor("s" + i, TSDataType.INT32, TSEncoding.RLE));
		// TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema,
		// TSFileDescriptor.getInstance().getConfig());
		writer = new TsFileIOWriter(outPutFile);
		memSize.set(0);
		workMemTable = new PrimitiveMemTable();
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= 500; i++) {
			for (int j = 1; j <= 100; j++) {
				for (long time = 1; time <= 22570; time++) {
					TSRecord record = new TSRecord(time, "d" + i);
					record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, "s" + j,
							String.valueOf(1000 + random.nextInt(100))));
					workMemTable.write(record.deltaObjectId, record.dataPointList.get(0).getMeasurementId(),
							record.dataPointList.get(0).getType(), record.time,
							record.dataPointList.get(0).getValue().toString());

					long mem = memSize.addAndGet(MemUtils.getRecordSize(record));
					// System.out.println(MemUtils.bytesCntToStr(mem));
					if (mem > memThreshold) {
						long flushStartTime = System.currentTimeMillis();
						long posStart = writer.getPos();
						MemTableFlushUtil.flushMemTable(fileSchema, writer, workMemTable);
						workMemTable = new PrimitiveMemTable();
						memSize.set(0);
						System.out.println(System.currentTimeMillis() - flushStartTime + " size: "
								+ MemUtils.bytesCntToStr(writer.getPos() - posStart));
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
		outPutFile.delete();
		fileSchema.registerMeasurement(new MeasurementDescriptor("s0", TSDataType.INT32, TSEncoding.RLE));
		// TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema,
		// TSFileDescriptor.getInstance().getConfig());
		writer = new TsFileIOWriter(outPutFile);
		memSize.set(0);
		workMemTable = new PrimitiveMemTable();
		long startTime = System.currentTimeMillis();
		for (int i = 1; i <= 112850000; i++) {
			TSRecord record = new TSRecord(i, "d0");
			record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, "s0", String.valueOf(1000 + random.nextInt(100))));
			workMemTable.write(record.deltaObjectId, record.dataPointList.get(0).getMeasurementId(),
					record.dataPointList.get(0).getType(), record.time,
					record.dataPointList.get(0).getValue().toString());
			long mem = memSize.addAndGet(MemUtils.getRecordSize(record));
			if (mem > memThreshold) {
				long flushStartTime = System.currentTimeMillis();
				long posStart = writer.getPos();
				MemTableFlushUtil.flushMemTable(fileSchema, writer, workMemTable);
				workMemTable = new PrimitiveMemTable();
				memSize.set(0);
				System.out.println(System.currentTimeMillis() - flushStartTime + " size: "
						+ MemUtils.bytesCntToStr(writer.getPos() - posStart));
			}
		}
		writer.endFile(fileSchema);
		long endTime = System.currentTimeMillis();
		System.out.println(String.format("write single series time cost %dms", endTime - startTime));
	}

}
