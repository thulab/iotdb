package cn.edu.thu.tsfile.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.*;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.hadoop.io.HDFSInputStream;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

/**
 * @author liukun
 */
public class TSFInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFInputFormat.class);

	/*
	 * For read time
	 */
	public static final String READ_TIME = "tsfile.read.time";
	/*
	 * For read deviceid
	 */
	public static final String READ_DELTAOBJECT = "tsfile.read.deltaObjectId";
	/*
	 * For the type of filter
	 */
	public static final String FILTER_TYPE = "tsfile.filter.type";
	/*
	 * For expression of filter
	 */
	public static final String FILTER_EXPRESSION = "tsfile.filter.expression";
	/*
	 * For has filter or not
	 */
	public static final String FILTER_EXIST = "tsfile.filter.exist";

	public static final String READ_DEVICES = "tsfile.read.deltaobject";
	public static final String READ_SENSORS = "tsfile.read.measurement";

	private static final String SPERATOR = ",";

	/**
	 * Set the devices which want to be read
	 *
	 * @param job
	 * @param value
	 * @throws TSFHadoopException
	 */
	public static void setReadDevices(Job job, String[] value) throws TSFHadoopException {
		if (value == null || value.length < 1) {
			throw new TSFHadoopException("The devices selected is null or empty");
		} else {
			String devices = "";
			for (String device : value) {
				devices = devices + device + SPERATOR;
			}
			// Get conf type
			job.getConfiguration().set(READ_DEVICES, (String) devices.subSequence(0, devices.length() - 1));
		}
	}

	/**
	 * Get the devices which want to be read
	 *
	 * @param configuration
	 * @return if don't set the devices, return null
	 */
	public static List<String> getReadDevices(Configuration configuration) {
		String devices = configuration.get(READ_DEVICES);
		if (devices == null || devices.length() < 1) {
			return null;
		} else {
			List<String> deviceList = Arrays.asList(devices.split(SPERATOR));
			return deviceList;
		}
	}

	/**
	 * Set the sensors which want to be read
	 *
	 * @param job
	 * @param value
	 * @throws TSFHadoopException
	 */
	public static void setReadSensors(Job job, String[] value) throws TSFHadoopException {
		if (value == null || value.length < 1) {
			throw new TSFHadoopException("The sensors selected is null or empty");
		} else {
			String sensors = "";
			for (String sensor : value) {
				sensors = sensors + sensor + SPERATOR;
			}
			// Get conf type
			job.getConfiguration().set(READ_SENSORS, (String) sensors.subSequence(0, sensors.length() - 1));
		}
	}

	/**
	 * Get the sensors which want to be read
	 *
	 * @param configuration
	 * @return if don't set the sensors, return null
	 */
	public static List<String> getReadSensors(Configuration configuration) {
		String sensors = configuration.get(READ_SENSORS);
		if (sensors == null || sensors.length() < 1) {
			return null;
		} else {
			List<String> sensorList = Arrays.asList(sensors.split(SPERATOR));
			return sensorList;
		}
	}

	/**
	 * @param job
	 * @param value
	 */
	public static void setReadDeltaObjectId(Job job, boolean value) {
		job.getConfiguration().setBoolean(READ_DELTAOBJECT, value);
	}

	/**
	 * @param configuration
	 * @return
	 */
	public static boolean getReadDeltaObject(Configuration configuration) {
		return configuration.getBoolean(READ_DELTAOBJECT, false);
	}

	/**
	 * @param job
	 * @param value
	 */
	public static void setReadTime(Job job, boolean value) {
		job.getConfiguration().setBoolean(READ_TIME, value);
	}

	public static boolean getReadTime(Configuration configuration) {
		return configuration.getBoolean(READ_TIME, false);
	}

	/**
	 * Set filter exist or not
	 *
	 * @param job
	 * @param value
	 */
	public static void setHasFilter(Job job, boolean value) {
		job.getConfiguration().setBoolean(FILTER_EXIST, value);
	}

	// check is we didn't set this key, the value will be null or empty

	/**
	 * Get filter exist or not
	 *
	 * @param configuration
	 * @return
	 */
	public static boolean getHasFilter(Configuration configuration) {
		return configuration.getBoolean(FILTER_EXIST, false);
	}

	/**
	 * @param job
	 * @param value
	 */
	public static void setFilterType(Job job, String value) {
		job.getConfiguration().set(FILTER_TYPE, value);
	}

	/**
	 * Get the filter type
	 *
	 * @param configuration
	 * @return
	 */
	// check if not set the filter type, the result will null or empty
	public static String getFilterType(Configuration configuration) {
		return configuration.get(FILTER_TYPE);
	}

	public static void setFilterExp(Job job, String value) {
		job.getConfiguration().set(FILTER_EXPRESSION, value);
	}

	public static String getFilterExp(Configuration configuration) {
		return configuration.get(FILTER_EXPRESSION);
	}

	@Override
	public RecordReader<NullWritable, ArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TSFRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration configuration = job.getConfiguration();
		BlockLocation[] blockLocations;
		List<InputSplit> splits = new ArrayList<>();
		// get the all file in the directory
		List<FileStatus> listFileStatus = super.listStatus(job);
		LOGGER.info("The number of this job file is {}", listFileStatus.size());
		//
		// system out
		//
		System.out.println("The directory has " + listFileStatus.size() + " files");
		// For each file
		for (FileStatus fileStatus : listFileStatus) {
			LOGGER.info("The file path is {}", fileStatus.getPath());
			//
			// system out
			//
			System.out.println("The file is " + fileStatus.getPath());
			// Get the file path
			Path path = fileStatus.getPath();
			// Get the file length
			long length = fileStatus.getLen();
			// Check the file length. if the length is less than 0, return the
			// empty splits
			if (length > 0) {
				// Get block information in the local file system or hdfs
				if (fileStatus instanceof LocatedFileStatus) {
					LOGGER.info("The file status is {}", LocatedFileStatus.class.getName());
					//
					// system out
					//
					System.out.println("The filestatus is " + LocatedFileStatus.class);
					blockLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
				} else {
					FileSystem fileSystem = path.getFileSystem(configuration);
					LOGGER.info("The file status is {}", fileStatus.getClass().getName());
					System.out.println("The file status is " + fileStatus.getClass().getName());
					System.out.println("The file system is " + fileSystem.getClass());
					blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, length);
				}
				LOGGER.info("The block location information is {}", Arrays.toString(blockLocations));
				//
				// system out
				//
				System.out.println("The block location information is " + Arrays.toString(blockLocations));
				HDFSInputStream hdfsInputStream = new HDFSInputStream(path, configuration);
				FileReader fileReader = new FileReader(hdfsInputStream);
				// Get the timeserise to test
				splits.addAll(generateSplits(path, fileReader, blockLocations));
				fileReader.close();
			} else {
				LOGGER.warn("The file length is " + length);
			}
		}
		configuration.setLong(NUM_INPUT_FILES, listFileStatus.size());
		LOGGER.info("The number of splits is " + splits.size());
		System.out.println("The number of splits is " + splits.size());

		return splits;
	}

	/**
	 * get the TSFInputSplit from tsfMetaData and hdfs block location
	 * information with the filter
	 *
	 * @param path
	 * @param fileReader
	 * @param blockLocations
	 * @return
	 * @throws IOException
	 */
	private List<TSFInputSplit> generateSplits(Path path, FileReader fileReader, BlockLocation[] blockLocations)
			throws IOException {
		Map<String, Integer> mapRowGroups = new HashMap<String, Integer>();
		List<TSFInputSplit> splits = new ArrayList<TSFInputSplit>();
		Comparator<BlockLocation> comparator = new Comparator<BlockLocation>() {
			@Override
			public int compare(BlockLocation o1, BlockLocation o2) {

				return Long.signum(o1.getOffset() - o2.getOffset());
			}

		};
		Arrays.sort(blockLocations, comparator);

		List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
		int currentBlockIndex = 0;
		long splitSize = 0;
		long splitStart = 0;
		List<String> hosts = new ArrayList<>();
		for (RowGroupMetaData rowGroupMetaData : fileReader.getSortedRowGroupMetaDataList()) {
			LOGGER.info("The rowGroupMetaData information is {}", rowGroupMetaData);

			long start = getRowGroupStart(rowGroupMetaData);
			int blkIndex = getBlockLocationIndex(blockLocations, start);
			if(hosts.size() == 0)
			{
				hosts.addAll(Arrays.asList(blockLocations[blkIndex].getHosts()));
				splitStart = start;
			}

			if(blkIndex != currentBlockIndex)
			{
				TSFInputSplit tsfInputSplit = makeSplit(path, rowGroupMetaDataList, splitStart,
						splitSize, hosts);
				LOGGER.info("The tsfile inputsplit information is {}", tsfInputSplit);
				splits.add(tsfInputSplit);

				currentBlockIndex = blkIndex;
				rowGroupMetaDataList.clear();
				rowGroupMetaDataList.add(rowGroupMetaData);
				splitStart = start;
				splitSize = rowGroupMetaData.getTotalByteSize();
				hosts.clear();
			}
			else
			{
				rowGroupMetaDataList.add(rowGroupMetaData);
				splitSize += rowGroupMetaData.getTotalByteSize();
			}
		}
		TSFInputSplit tsfInputSplit = makeSplit(path, rowGroupMetaDataList, splitStart,
				splitSize, hosts);
		LOGGER.info("The tsfile inputsplit information is {}", tsfInputSplit);
		splits.add(tsfInputSplit);
		return splits;
	}

	private long getRowGroupStart(RowGroupMetaData rowGroupMetaData) {
		return rowGroupMetaData.getMetaDatas().get(0).getProperties().getFileOffset();
	}

	private int getBlockLocationIndex(BlockLocation[] blockLocations, long start) {
		for (int i = 0; i < blockLocations.length; i++) {
			if (blockLocations[i].getOffset() <= start
					&& start < blockLocations[i].getOffset() + blockLocations[i].getLength()) {
				return i;
			}
		}
		LOGGER.warn(String.format("Can't find the block. The start is:%d. the last block is", start),
				blockLocations[blockLocations.length - 1].getOffset()
						+ blockLocations[blockLocations.length - 1].getLength());
		return -1;
	}

	private TSFInputSplit makeSplit(Path path, List<RowGroupMetaData> rowGroupMataDataList, long start, long length,
			List<String> hosts) {
		String[] hosts_str = hosts.toArray(new String[hosts.size()]);
		return new TSFInputSplit(path, rowGroupMataDataList, start, length, hosts_str);
	}
}
