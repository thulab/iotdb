package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;
import java.util.*;

import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.hadoop.io.HDFSInputStream;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryConfig;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.HadoopQueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import sun.rmi.runtime.Log;

/**
 * @author liukun
 */
public class TSFRecordReader extends RecordReader<NullWritable, ArrayWritable> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFRecordReader.class);

	private QueryDataSet dataSet = null;
	private List<Field> fields = null;
	private long timestamp = 0;
	private String deviceId;
	private int sensorNum = 0;
	private int sensorIndex = 0;
	private boolean isReadDeviceId = false;
	private boolean isReadTime = false;
	private int arraySize = 0;
	
	private HDFSInputStream hdfsInputStream;

	private static final String SEPARATOR_DEVIDE_SERIES = ".";
	private static final String SEPARATOR_SERIES = "|";

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.
	 * mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		if (split instanceof TSFInputSplit) {
			LOGGER.info("Current split is a TSFSplit,start to initialize~");

			TSFInputSplit tsfInputSplit = (TSFInputSplit) split;
			Path path = tsfInputSplit.getPath();
			List<RowGroupMetaData> rowGroupMetaDataList = tsfInputSplit.getDeviceRowGroupMetaDataList();
			Configuration configuration = context.getConfiguration();
			hdfsInputStream = new HDFSInputStream(path, configuration);
			// Get the read columns and filter information
			List<String> deviceIdList = TSFInputFormat.getReadDevices(configuration);
			if(deviceIdList == null)deviceIdList = initDeviceIdList(rowGroupMetaDataList);
			List<String> sensorIdList = TSFInputFormat.getReadSensors(configuration);
			if(sensorIdList == null)sensorIdList = initSensorIdList(rowGroupMetaDataList);
			LOGGER.info("Devices:" + deviceIdList);
			LOGGER.info("Sensors:" + sensorIdList);

			this.sensorNum = sensorIdList.size();
			isReadDeviceId = TSFInputFormat.getReadDeltaObject(configuration);
			isReadTime = TSFInputFormat.getReadTime(configuration);
			if (isReadDeviceId) {
				arraySize++;
			}
			if (isReadTime) {
				arraySize++;
			}
			arraySize += sensorNum;

			HadoopQueryEngine queryEngine = new HadoopQueryEngine(hdfsInputStream, rowGroupMetaDataList);
			dataSet = queryEngine.queryWithSpecificRowGroups(deviceIdList, sensorIdList, null, null, null);
			LOGGER.info("Initialization complete~");
		} else {
			LOGGER.error("The InputSplit class is not {}, the class is {}", TSFInputSplit.class.getName(),
					split.getClass().getName());
			throw new InternalError(String.format("The InputSplit class is not %s, the class is %s",
					TSFInputSplit.class.getName(), split.getClass().getName()));
		}
	}

	private List<String> initDeviceIdList(List<RowGroupMetaData> rowGroupMetaDataList) {
		Set<String> deviceIdSet = new HashSet<>();
		for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
			deviceIdSet.add(rowGroupMetaData.getDeltaObjectID());
		}
		return new ArrayList<>(deviceIdSet);
	}

	private List<String> initSensorIdList(List<RowGroupMetaData> rowGroupMetaDataList){
		Set<String> sensorIdSet = new HashSet<>();
		for(RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
			for(TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()){
				sensorIdSet.add(timeSeriesChunkMetaData.getProperties().getMeasurementUID());
			}
		}
		return new ArrayList<>(sensorIdSet);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		LOGGER.info("Start to get next data~");
		sensorIndex += sensorNum;

		if(fields == null || sensorIndex >= fields.size()){
			LOGGER.info("Start another row~");
			if(!dataSet.next()){
				LOGGER.info("Finish all rows~");
				return false;
			}

			RowRecord rowRecord = dataSet.getCurrentRecord();
			fields = rowRecord.getFields();
			timestamp = rowRecord.getTime();
			sensorIndex = 0;
			LOGGER.info("New time is " + timestamp);
			LOGGER.info("New fields are " + fields);
		}
		deviceId = fields.get(sensorIndex).deltaObjectId;

		return true;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
		LOGGER.info("Start to get current value~");

		Writable[] writables = getEmptyWritables();
		Text deviceid = new Text(deviceId);
		LongWritable time = new LongWritable(timestamp);
		LOGGER.info("device:" + deviceId + "\ttimestamp:" + time);
		int index = 0;
		if (isReadTime && isReadDeviceId) {
			writables[0] = time;
			writables[1] = deviceid;
			index = 2;
		} else if (isReadTime && !isReadDeviceId) {
			writables[0] = time;
			index = 1;
		} else if (!isReadTime && isReadDeviceId) {
			writables[0] = deviceid;
			index = 1;
		}

		LOGGER.info("Start to get real data, sensor index is " + sensorIndex + ", sensor num is " + sensorNum);
		LOGGER.info("Fields are:" + fields);
		for(int i = 0;i < sensorNum;i++)
		{
			LOGGER.info("Current index is " + i);
			Field field = fields.get(sensorIndex + i);
			if (field.isNull()) {
				LOGGER.info("Current value is null");
				writables[index] = NullWritable.get();
			} else {
				LOGGER.info("Current type is " + field.dataType);
				switch (field.dataType) {
				case INT32:
					writables[index] = new IntWritable(field.getIntV());
					break;
				case INT64:
					writables[index] = new LongWritable(field.getLongV());
					break;
				case FLOAT:
					writables[index] = new FloatWritable(field.getFloatV());
					break;
				case DOUBLE:
					writables[index] = new DoubleWritable(field.getDoubleV());
					break;
				case BOOLEAN:
					writables[index] = new BooleanWritable(field.getBoolV());
					break;
				case TEXT:
					writables[index] = new Text(field.getBinaryV().getStringValue());
					break;
				default:
					LOGGER.error("The data type is not support {}", field.dataType);
					throw new InterruptedException(String.format("The data type %s is not support ", field.dataType));
				}
			}
			LOGGER.info("Get:" + writables[index]);
			index++;
		}
		LOGGER.info("Get array:" + writables);
		return new ArrayWritable(Writable.class, writables);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	@Override
	public void close() throws IOException {
		dataSet = null;
		hdfsInputStream.close();
	}

	private Writable[] getEmptyWritables() {
		Writable[] writables = new Writable[arraySize];
		return writables;
	}
}
