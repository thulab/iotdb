package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MemTableFlushUtil {
	private static final Logger logger = LoggerFactory.getLogger(MemTableFlushUtil.class);

	public static void flushMemTable(FileSchema fileSchema, TsFileIOWriter tsFileIOWriter, IMemTable iMemTable,
			int pageSizeThreshold) throws IOException {
		for (String deltaObjectId : iMemTable.getMemTableMap().keySet()) {
			long startPos = tsFileIOWriter.getPos();
			long recordCount = 0;
			tsFileIOWriter.startRowGroup(deltaObjectId);
			for (String measurementId : iMemTable.getMemTableMap().get(deltaObjectId).keySet()) {
				IMemSeries series = iMemTable.getMemTableMap().get(deltaObjectId).get(measurementId);
				MeasurementDescriptor desc = fileSchema.getMeasurementDescriptor(measurementId);
				IPageWriter pageWriter = new PageWriterImpl(desc);
				SeriesWriterImpl seriesWriter = new SeriesWriterImpl(deltaObjectId, desc, pageWriter,
						pageSizeThreshold);
				for (TreeSetMemSeries.TimeValuePairInMemTable tvPair : series.query()) {
					recordCount++;
					switch (desc.getType()) {
					case BOOLEAN:
						seriesWriter.write(tvPair.getTimestamp(), tvPair.getValue().getBoolean());
						break;
					case INT32:
						seriesWriter.write(tvPair.getTimestamp(), tvPair.getValue().getInt());
						break;
					case INT64:
						seriesWriter.write(tvPair.getTimestamp(), tvPair.getValue().getLong());
						break;
					case FLOAT:
						seriesWriter.write(tvPair.getTimestamp(), tvPair.getValue().getFloat());
						break;
					case DOUBLE:
						seriesWriter.write(tvPair.getTimestamp(), tvPair.getValue().getDouble());
						break;
					case TEXT:
						seriesWriter.write(tvPair.getTimestamp(), tvPair.getValue().getBinary());
						break;
					default:
						logger.error("don't support data type: {}", desc.getType());
					}
				}
				seriesWriter.writeToFileWriter(tsFileIOWriter);
			}
			long memSize = tsFileIOWriter.getPos() - startPos;
			tsFileIOWriter.endRowGroup(memSize, recordCount);
		}
	}
}
