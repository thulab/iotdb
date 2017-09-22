package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

/**
 * @author liukun
 */
public class TSFRecordReader extends RecordReader<NullWritable, ArrayWritable> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFRecordReader.class);

	private QueryDataSet dataSet = null;
	private String deviceId = null;
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
			TSFInputSplit tsfInputSplit = (TSFInputSplit) split;
			Path path = tsfInputSplit.getPath();
			deviceId = tsfInputSplit.getDeviceId();
			int rowGroupIndex = tsfInputSplit.getIndexOfDeviceRowGroup();
			Configuration configuration = context.getConfiguration();
			hdfsInputStream = new HDFSInputStream(path, configuration);
			// Get the read columns and filter information
			String[] columns = TSFInputFormat.getReadColumns(configuration);
			isReadDeviceId = TSFInputFormat.getReadDeltaObject(configuration);
			isReadTime = TSFInputFormat.getReadTime(configuration);
			// Don't read any columns( series, time, deviceid)
			if ((columns == null || columns.length < 1) && (isReadDeviceId == false && isReadTime == false)) {
				LOGGER.error("The read columns is null or empty");
				throw new InterruptedException("The read columns is null or empty");
			}
			if (isReadDeviceId) {
				arraySize++;
			}
			if (isReadTime) {
				arraySize++;
			}
			// Just read time or deviceid
			if (columns == null || columns.length < 1) {
				FileReader fileReader = new FileReader(hdfsInputStream);
				List<TimeSeriesMetadata> series = fileReader.getFileMetadata().getTimeSeriesList();
				List<String> seriesName = new ArrayList<>();
				for (TimeSeriesMetadata timeSeries : series) {
					seriesName.add(timeSeries.getMeasurementUID());
				}
				columns = seriesName.toArray(new String[seriesName.size()]);
			}
			arraySize += columns.length;
			// Read data using query engine
			String selectColumns = "";
			for (int i = 0; i < columns.length; i++) {
				selectColumns = selectColumns + deviceId + SEPARATOR_DEVIDE_SERIES + columns[i] + SEPARATOR_SERIES;
			}
			selectColumns = selectColumns.substring(0, selectColumns.length() - 1);
			QueryConfig queryConfig = new QueryConfig(selectColumns);
			QueryEngine queryEngine = new QueryEngine(hdfsInputStream);
			dataSet = queryEngine.queryInOneRowGroup(queryConfig, rowGroupIndex);
		} else {
			LOGGER.error("The InputSplit class is not {}, the class is {}", TSFInputSplit.class.getName(),
					split.getClass().getName());
			throw new InternalError(String.format("The InputSplit class is not %s, the class is %s",
					TSFInputSplit.class.getName(), split.getClass().getName()));
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return dataSet.next();
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
		RowRecord rowRecord = dataSet.getCurrentRecord();
		Writable[] writables = getEmptyWritables();
		Text deviceid = new Text(deviceId);
		LongWritable time = new LongWritable(rowRecord.getTime());
		int offect = 0;
		if (isReadTime && isReadDeviceId) {
			writables[0] = time;
			writables[1] = deviceid;
			offect = 2;
		} else if (isReadTime && !isReadDeviceId) {
			writables[0] = time;
			offect = 1;
		} else if (!isReadTime && isReadDeviceId) {
			writables[0] = deviceid;
			offect = 1;
		}
		List<Field> fields = rowRecord.getFields();
		for (Field field : fields) {
			if (field.isNull()) {
				writables[offect] = NullWritable.get();
			} else {
				switch (field.dataType) {
				case INT32:
					writables[offect] = new IntWritable(field.getIntV());
					break;
				case INT64:
					writables[offect] = new LongWritable(field.getLongV());
					break;
				case FLOAT:
					writables[offect] = new FloatWritable(field.getFloatV());
					break;
				case DOUBLE:
					writables[offect] = new DoubleWritable(field.getDoubleV());
					break;
				default:
					LOGGER.error("The data type is not support {}", field.dataType);
					throw new InterruptedException(String.format("The data type %s is not support ", field.dataType));
				}
			}
			offect++;
		}
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
