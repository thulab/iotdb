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
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;

public class MergeWriteWithSeriesWriterPerfTest {

	private static final String outPutFilePath = "output";
	private static long memThreshold = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
	private static AtomicLong memSize = new AtomicLong();
	private static Random random = new Random(System.currentTimeMillis());
	private static IMemTable workMemTable;
	private static TsFileIOWriter writer;

	public static void main(String[] args) throws WriteProcessException, IOException {
		writeMultiSeriesTest();
		// writeSingleSeriesTest();
	}

	private static void writeMultiSeriesTest() throws WriteProcessException, IOException {
		FileSchema fileSchema = new FileSchema();
		File outPutFile = new File(outPutFilePath);
		outPutFile.delete();
		for (int i = 1; i <= 1000; i++)
			fileSchema.registerMeasurement(new MeasurementDescriptor("s" + i, TSDataType.INT32, TSEncoding.RLE));
		// TsFileWriter fileWriter = new TsFileWriter(outPutFile, fileSchema,
		// TSFileDescriptor.getInstance().getConfig());
		writer = new TsFileIOWriter(outPutFile);
		memSize.set(0);
		long startTime = System.currentTimeMillis();
		long disksize = 0;
		for (int i = 1; i <= 500; i++) {
			String deltaObjectId = "d" + i;
			long recordCount = 0;
			long startPos = writer.getPos();
			writer.startRowGroup(deltaObjectId);
			for (int j = 1; j <= 100; j++) {
				String measurementId = "s" + j;
				MeasurementDescriptor desc = fileSchema.getMeasurementDescriptor(measurementId);
				IPageWriter pageWriter = new PageWriterImpl(desc);
				SeriesWriterImpl seriesWriter = new SeriesWriterImpl(deltaObjectId, desc, pageWriter, 64 * 1024);
				TSDataType dataType = desc.getType();
				for (long time = 1; time <= 2257*10; time++) {
					recordCount++;
					switch (dataType) {
					case INT64:
						break;
					case FLOAT:
						break;
					case DOUBLE:
						break;
					case TEXT:
						break;
					case INT32:
						seriesWriter.write(time, 1000 + random.nextInt(100));
						break;
					default:
						break;
					}
				}
				seriesWriter.writeToFileWriter(writer);
			}
			long memSize = writer.getPos() - startPos;
			disksize += memSize;
			writer.endRowGroup(memSize, recordCount);
		}
		writer.endFile(fileSchema);
		long endTime = System.currentTimeMillis();
		System.out.println(String.format("write multi series time cost %dms", endTime - startTime));
		System.out.println(String.format("file size is %s", MemUtils.bytesCntToStr(disksize)));
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
