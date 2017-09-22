package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.hadoop.io.HDFSOutputStream;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.timeseries.write.InternalRecordWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.tsinghua.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.WriteSupport;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class TSFRecordWriter extends RecordWriter<NullWritable, TSRow> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFRecordWriter.class);

	private InternalRecordWriter<TSRecord> write = null;

	public TSFRecordWriter(Path path, JSONObject schema) throws InterruptedException, IOException {
		// construct the internalrecordwriter
		FileSchema fileSchema = null;
		try {
			fileSchema = new FileSchema(schema);
		} catch (InvalidJsonSchemaException e) {
			e.printStackTrace();
			LOGGER.error("Construct the tsfile schema failed, the reason is {}", e.getMessage());
			throw new InterruptedException(e.getMessage());
		}

		HDFSOutputStream hdfsOutputStream = new HDFSOutputStream(path, new Configuration(), false);

		WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();

		TSFileIOWriter tsfileWriter = new TSFileIOWriter(fileSchema, hdfsOutputStream);

		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();

		if (schema.has(JsonFormatConstant.ROW_GROUP_SIZE))
			conf.groupSizeInByte = schema.getInt(JsonFormatConstant.ROW_GROUP_SIZE);
		if (schema.has(JsonFormatConstant.PAGE_SIZE))
			conf.pageSizeInByte = schema.getInt(JsonFormatConstant.PAGE_SIZE);

		write = new TSRecordWriter(conf, tsfileWriter, writeSupport, fileSchema);
	}

	@Override
	public void write(NullWritable key, TSRow value) throws IOException, InterruptedException {

		try {
			write.write(value.getRow());
		} catch (WriteProcessException e) {
			e.printStackTrace();
			LOGGER.error("Write tsfile record error, the error message is {}", e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {

		LOGGER.info("Close the recordwriter, the task attempt id is {}", context.getTaskAttemptID());
		write.close();
	}
	
	

}
