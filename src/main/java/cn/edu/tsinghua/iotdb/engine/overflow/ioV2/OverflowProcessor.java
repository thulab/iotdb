package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.utils.FlushState;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class OverflowProcessor extends Processor {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private OverflowResource workResource;
	private OverflowResource mergeResource;

	private OverflowSupport workSupport;
	private OverflowSupport flushSupport;

	private FlushState flushStatus;
	private boolean isMerge;
	private ReentrantLock queryFlushLock = new ReentrantLock();
	private int valueCount;
	private String parentPath;
	private AtomicLong dataPahtCount = new AtomicLong();

	public OverflowProcessor(String processorName, Map<String, Object> parameters) {
		super(processorName);
		String overflowDirPath = TsFileDBConf.overflowDataDir;
		if (overflowDirPath.length() > 0
				&& overflowDirPath.charAt(overflowDirPath.length() - 1) != File.separatorChar) {
			overflowDirPath = overflowDirPath + File.separatorChar;
		}
		this.parentPath = overflowDirPath + processorName;
		File processorDataDir = new File(parentPath);
		if (!processorDataDir.exists()) {
			processorDataDir.mkdirs();
		}
		// recover file
		recovery(processorDataDir);
		// recover memory
		workSupport = new OverflowSupport();
	}

	private void recovery(File parentFile) {
		String[] subFilePaths = parentFile.list();
		if (subFilePaths.length == 0) {
			workResource = new OverflowResource(parentPath, String.valueOf(dataPahtCount.getAndIncrement()));
			return;
		} else if (subFilePaths.length == 1) {
			long count = Long.valueOf(subFilePaths[0]) + 1;
			dataPahtCount.addAndGet(count);
			workResource = new OverflowResource(parentPath, String.valueOf(count));
			LOGGER.info("The overflow processor {} recover from work status.", getProcessorName());
		} else {
			long count1 = Long.valueOf(subFilePaths[0]);
			long count2 = Long.valueOf(subFilePaths[1]);
			if (count1 > count2) {
				long temp = count1;
				count1 = count2;
				count2 = temp;
			}
			dataPahtCount.addAndGet(count2 + 1);
			// work dir > merge dir
			workResource = new OverflowResource(parentPath, String.valueOf(count2));
			mergeResource = new OverflowResource(parentPath, String.valueOf(count1));
			LOGGER.info("The overflow processor {} recover from merge status.", getProcessorName());
		}
	}

	public void insert(TSRecord tsRecord) {
		workSupport.insert(tsRecord);
		valueCount++;
	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			byte[] value) {
		workSupport.update(deltaObjectId, measurementId, startTime, endTime, type, value);
		valueCount++;
	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type) {
		workSupport.delete(deltaObjectId, measurementId, timestamp, type);
		valueCount++;
	}

	public void query(String deltaObjectId, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, TSDataType dataType)
			throws IOException {
		queryFlushLock.lock();
		try {
			// flush lock / merge lock
			// query insert data in memory and unseqTsFiles
			queryOverflowInsertInMemory(deltaObjectId, measurementId, timeFilter, freqFilter, valueFilter, dataType);
			queryWorkDataInOverflowInsert(deltaObjectId, measurementId, dataType);
			// TODO: check merge
			queryMergeDataInOverflowInsert(deltaObjectId, measurementId, dataType);
			// query update/delete data in memory and overflowFiles
			DynamicOneColumnData updateDataInMem = queryOverflowUpdateInMemory(deltaObjectId, measurementId, timeFilter,
					freqFilter, valueFilter, dataType);
			queryWorkDataInOverflowUpdate(deltaObjectId, measurementId, dataType);
			// TODO: check merge
			queryMergeDataInOverflowUpdate(deltaObjectId, measurementId, dataType);
			// return overflow query struct
		} finally {
			queryFlushLock.unlock();
		}
	}

	private void queryOverflowInsertInMemory(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType) {
		// query memtable
		workSupport.queryOverflowInsertInMemory(deltaObjectId, measurementId, dataType);
		if (flushStatus.isFlushing()) {
			flushSupport.queryOverflowInsertInMemory(deltaObjectId, measurementId, dataType);
		}
		// TODO: merge two result
		// TODO: return result
	}

	private DynamicOneColumnData queryOverflowUpdateInMemory(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType) {
		DynamicOneColumnData columnData = workSupport.queryOverflowUpdateInMemory(deltaObjectId, measurementId,
				timeFilter, freqFilter, valueFilter, dataType, null);
		if (flushStatus.isFlushing()) {
			columnData = flushSupport.queryOverflowUpdateInMemory(deltaObjectId, measurementId, timeFilter, freqFilter,
					valueFilter, dataType, columnData);
		}
		return columnData;
	}

	/**
	 * Get the update/delete data which is WORK in overflowFile.
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of overflowFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryWorkDataInOverflowUpdate(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				workResource.getUpdateDeleteFilePath(),
				workResource.getUpdateDeleteMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	/**
	 * Get the insert data which is WORK in unseqTsFile.
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of unseqTsFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryWorkDataInOverflowInsert(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				workResource.getInsertFilePath(),
				workResource.getInsertMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	/**
	 * Get the all merge data in unseqTsFile and overflowFile
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 */
	public void queryMerge(String deltaObjectId, String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> mergeInsert = queryMergeDataInOverflowInsert(deltaObjectId,
				measurementId, dataType);
		Pair<String, List<TimeSeriesChunkMetaData>> mergeUpdate = queryMergeDataInOverflowUpdate(deltaObjectId,
				measurementId, dataType);
	}

	/**
	 * Get the update/delete data which is MERGE in overflowFile.
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of overflowFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryMergeDataInOverflowUpdate(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				mergeResource.getUpdateDeleteFilePath(),
				mergeResource.getUpdateDeleteMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	/**
	 * Get the insert data which is MERGE in unseqTsFile
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @param dataType
	 * @return the path of unseqTsFile, List of TimeSeriesChunkMetaData for the
	 *         special time-series.
	 */
	private Pair<String, List<TimeSeriesChunkMetaData>> queryMergeDataInOverflowInsert(String deltaObjectId,
			String measurementId, TSDataType dataType) {
		Pair<String, List<TimeSeriesChunkMetaData>> pair = new Pair<String, List<TimeSeriesChunkMetaData>>(
				mergeResource.getInsertFilePath(),
				mergeResource.getInsertMetadatas(deltaObjectId, measurementId, dataType));
		return pair;
	}

	private void switchWorkToFlush() {
		queryFlushLock.lock();
		try {
			flushSupport = workSupport;
			workSupport = new OverflowSupport();
		} finally {
			queryFlushLock.unlock();
		}
	}

	private void switchFlushToWork() {
		queryFlushLock.lock();
		try {
			flushSupport.clear();
			flushSupport = null;
		} finally {
			queryFlushLock.unlock();
		}
	}

	public void switchWorkToMerge() {
		if (mergeResource == null) {
			mergeResource = workResource;
			// TODO: NEW ONE workResource
			workResource = new OverflowResource(parentPath, String.valueOf(dataPahtCount.getAndIncrement()));
		}
		isMerge = true;
	}

	public void switchMergeToWork() throws IOException {
		if (mergeResource != null) {
			mergeResource.close();
			mergeResource = null;
		}
		isMerge = false;
	}

	public boolean isMerge() {
		return isMerge;
	}

	public boolean isFlush() {
		synchronized (flushStatus) {
			return flushStatus.isFlushing();
		}
	}

	private Future<?> flush(boolean synchronization) {
		// statistic information for flush

		// value count

		// while check flush

		// prepare for flushing

		// check synchronization
		if (synchronization) {

		} else {

		}
		return null;
		// return release the lock
	}

	@Override
	public void flush() {

		// TODO: USE FUTURE TO CONTROL SYNCHRONIZATION OR ASYNCHRONIZATION
		// flush interval

		//
	}

	@Override
	public void close() {

	}

	@Override
	public boolean canBeClosed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long memoryUsage() {
		// TODO Auto-generated method stub
		return 0;
	}
}
