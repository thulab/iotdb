package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
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

import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.TSFileMetaData;
import cn.edu.thu.tsfile.hadoop.io.HDFSInputStream;
import cn.edu.thu.tsfile.timeseries.read.FileReader;

/**
 * @author liukun
 */
public class TSFInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFInputFormat.class);

	/*
	 * For read columns selected
	 */
	public static final String READ_COLUMNS = "tsfile.read.columns";
	/*
	 * For read time
	 */
	public static final String READ_TIME = "tsfile.read.time";
	/*
	 * For read deviceid
	 */
	public static final String READ_DEVICEID = "tsfile.read.deviceid";
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

	private static final String SPERATOR = ",";

	/**
	 * Set the columns which want to be read
	 *
	 * @param job
	 * @param value
	 * @throws TSFHadoopException
	 */
	public static void setReadColumns(Job job, String[] value) throws TSFHadoopException {
		if (value == null || value.length < 1) {
			throw new TSFHadoopException("The columns selected is null or empty");
		} else {
			String columns = "";
			for (String column : value) {
				columns = columns + column + ",";
			}
			// Get conf type
			System.out.println(job.getConfiguration().getClass());
			job.getConfiguration().set(READ_COLUMNS, (String) columns.subSequence(0, columns.length() - 1));
		}
	}

	/**
	 * Get the columns which want to be read
	 *
	 * @param configuration
	 * @return if don't set the columns, return null
	 */
	public static String[] getReadColumns(Configuration configuration) {
		String columns = configuration.get(READ_COLUMNS);
		if (columns == null || columns.length() < 1) {
			return null;
		} else {
			String[] cols = columns.split(SPERATOR);
			return cols;
		}
	}

	/**
	 * @param job
	 * @param value
	 */
	public static void setReadDeviceId(Job job, boolean value) {
		job.getConfiguration().setBoolean(READ_DEVICEID, value);
	}

	/**
	 * @param configuration
	 * @return
	 */
	public static boolean getReadDeviceId(Configuration configuration) {
		return configuration.getBoolean(READ_DEVICEID, false);
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
				TSFileMetaData tsfMetaData = fileReader.getFileMetadata();
				// Get the timeserise to test
				splits.addAll(generateSplits(path, tsfMetaData, blockLocations));
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
	 * @param tsfMetaData
	 * @param blockLocations
	 * @return
	 * @throws IOException
	 */
	private List<TSFInputSplit> generateSplits(Path path, TSFileMetaData tsfMetaData, BlockLocation[] blockLocations)
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

		int rowGroupIndex = 0;
		for (RowGroupMetaData rowGroupMetaData : tsfMetaData.getRowGroups()) {
			LOGGER.info("The rowGroupMetaData information is {}", rowGroupMetaData);
			String deviceId = rowGroupMetaData.getDeltaObjectUID();
			long start = getRowGroupStart(rowGroupMetaData);
			int blkIndex = getBlockLocationIndex(blockLocations, start);
			String[] hosts = null;
			if (blkIndex >= 0) {
				try {
					hosts = blockLocations[blkIndex].getHosts();
				} catch (IOException e) {
					e.printStackTrace();
					throw e;
				}
			} else {
				try {
					hosts = blockLocations[0].getHosts();
				} catch (IOException e) {
					e.printStackTrace();
					throw e;
				}
			}
			/*
			 * if (mapRowGroups.containsKey(deviceId)) { int index =
			 * mapRowGroups.get(deviceId); mapRowGroups.put(deviceId, index +
			 * 1); // check the filter is null
			 * LOGGER.info("The split information:" + "\n" + "path:" +
			 * path.toString() + "\t" + "deviceId:" + deviceId + "\t" + "Index:"
			 * + index + "\t" + "start:" + start + "\t" + "length:" +
			 * rowGroupMetaData.getTotalByteSize() + "\t" + "hosts:" + hosts);
			 * splits.add(makeSplit(path, deviceId, index, start,
			 * rowGroupMetaData.getTotalByteSize(), hosts)); } else {
			 * mapRowGroups.put(deviceId, 1);
			 * LOGGER.info("The split information:" + "\n" + "path:" +
			 * path.toString() + "\t" + "deviceId:" + deviceId + "\t" + "Index:"
			 * + 0 + "\t" + "start:" + start + "\t" + "length:" +
			 * rowGroupMetaData.getTotalByteSize() + "\t" + "hosts:" + hosts);
			 * splits.add(makeSplit(path, deviceId, 0, start,
			 * rowGroupMetaData.getTotalByteSize(), hosts)); }
			 */
			TSFInputSplit tsfInputSplit = makeSplit(path, deviceId, rowGroupIndex, start,
					rowGroupMetaData.getTotalByteSize(), hosts);
			LOGGER.info("The tsfile inputsplit information is {}", tsfInputSplit);
			splits.add(tsfInputSplit);
			rowGroupIndex++;
		}
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

	private TSFInputSplit makeSplit(Path path, String deviceUID, int numOfRowGroupMetaDate, long start, long length,
			String[] hosts) {
		return new TSFInputSplit(path, deviceUID, numOfRowGroupMetaDate, start, length, hosts);
	}

}
