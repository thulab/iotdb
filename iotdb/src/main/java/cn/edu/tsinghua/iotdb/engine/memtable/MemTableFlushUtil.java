package cn.edu.tsinghua.iotdb.engine.memtable;

import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.footer.ChunkGroupFooter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ChunkBuffer;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ChunkWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.series.IChunkWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.PageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
<<<<<<< HEAD
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
=======
import cn.edu.tsinghua.tsfile.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.write.series.SeriesWriterImpl;
>>>>>>> origin/kill_thanos

public class MemTableFlushUtil {
	private static final Logger logger = LoggerFactory.getLogger(MemTableFlushUtil.class);
	private static final int pageSizeThreshold = TSFileDescriptor.getInstance().getConfig().pageSizeInByte;

	private static int writeOneSeries(List<TimeValuePair> tvPairs, IChunkWriter seriesWriterImpl,
			TSDataType dataType) throws IOException {
		int count = 0;
		switch (dataType) {
		case BOOLEAN:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
			}
			break;
		case INT32:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
			}
			break;
		case INT64:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
			}
			break;
		case FLOAT:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
			}
			break;
		case DOUBLE:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
			}
			break;
		case TEXT:
			for (TimeValuePair timeValuePair : tvPairs) {
				count++;
				seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
			}
			break;
		default:
			logger.error("don't support data type: {}", dataType);
			break;
		}
		return count;
	}

	public static void flushMemTable(FileSchema fileSchema, TsFileIOWriter tsFileIOWriter, IMemTable iMemTable)
			throws IOException {
		for (String deltaObjectId : iMemTable.getMemTableMap().keySet()) {
			long startPos = tsFileIOWriter.getPos();
			long recordCount = 0;
			ChunkGroupFooter chunkGroupFooter = tsFileIOWriter.startFlushChunkGroup(deltaObjectId,0,
					iMemTable.getMemTableMap().get(deltaObjectId).size());
			for (String measurementId : iMemTable.getMemTableMap().get(deltaObjectId).keySet()) {
				IMemSeries series = iMemTable.getMemTableMap().get(deltaObjectId).get(measurementId);
				MeasurementSchema desc = fileSchema.getMeasurementSchema(measurementId);
				PageWriter pageWriter = new PageWriter(desc);
				ChunkBuffer chunkBuffer = new ChunkBuffer(desc);
				IChunkWriter seriesWriter = new ChunkWriterImpl(desc,chunkBuffer, pageSizeThreshold);
				recordCount += writeOneSeries(series.getSortedTimeValuePairList(), seriesWriter, desc.getType());
				seriesWriter.writeToFileWriter(tsFileIOWriter);
			}
			long memSize = tsFileIOWriter.getPos() - startPos;
			chunkGroupFooter.setDataSize(memSize);
			tsFileIOWriter.endChunkGroup(chunkGroupFooter);
		}
	}
}
