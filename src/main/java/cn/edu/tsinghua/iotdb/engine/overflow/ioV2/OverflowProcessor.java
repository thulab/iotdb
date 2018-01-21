package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.flushthread.FlushManager;
import cn.edu.tsinghua.iotdb.engine.utils.FlushStatus;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class OverflowProcessor extends Processor {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private OverflowResource workResource;
	private OverflowResource mergeResource;

	private OverflowSupport workSupport;
	private OverflowSupport flushSupport;

	private volatile FlushStatus flushStatus = new FlushStatus();
	private volatile boolean isMerge;
	private int valueCount;
	private String parentPath;
	private long lastFlushTime = -1;
	private AtomicLong dataPahtCount = new AtomicLong();
	private ReentrantLock queryFlushLock = new ReentrantLock();

	private Action overflowFlushAction = null;
	private Action filenodeFlushAction = null;
	private FileSchema fileSchema;

	public OverflowProcessor(String processorName, Map<String, Object> parameters, FileSchema fileSchema) {
		super(processorName);
		this.fileSchema = fileSchema;
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
		overflowFlushAction = (Action) parameters.get(FileNodeConstants.OVERFLOW_FLUSH_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);
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
			isMerge = true;
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
		if (!isMerge) {
			return new Pair<String, List<TimeSeriesChunkMetaData>>(null, null);
		}
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
		if (!isMerge) {
			return new Pair<String, List<TimeSeriesChunkMetaData>>(null, null);
		}
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
			mergeResource.deleteResource();
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

	private void flushOperation(String flushFunction) {
		long flushStartTime = System.currentTimeMillis();
		try {
			LOGGER.info("The overflow processor {} starts flushing {}.", getProcessorName(), flushFunction);
			// flush data
			workResource.flush(this.fileSchema, flushSupport.getMemTabale(), flushSupport.getOverflowSeriesMap());
			filenodeFlushAction.act();
			// write-ahead log
			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				WriteLogManager.getInstance().endOverflowFlush(getProcessorName());
			}
		} catch (IOException e) {
			LOGGER.error("Flush overflow processor {} rowgroup to file error in {}. Thread {} exits.",
					getProcessorName(), flushFunction, Thread.currentThread().getName(), e);
		} catch (Exception e) {
			LOGGER.error("FilenodeFlushAction action failed. Thread {} exits.", Thread.currentThread().getName(), e);
		} finally {
			synchronized (flushStatus) {
				flushStatus.setUnFlushing();
				// switch from flush to work.
				switchFlushToWork();
				flushStatus.notify();
			}
			// BasicMemController.getInstance().reportFree(this,
			// oldMemUsage);
		}
		// log flush time
		LOGGER.info("The overflow processor {} ends flushing {}.", getProcessorName(), flushFunction);
		long flushEndTime = System.currentTimeMillis();
		long timeInterval = flushEndTime - flushStartTime;
		DateTime startDateTime = new DateTime(flushStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		DateTime endDateTime = new DateTime(flushEndTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		LOGGER.info(
				"The overflow processor {} flush {}, start time is {}, flush end time is {}, time consumption is {}ms",
				getProcessorName(), flushFunction, startDateTime, endDateTime, timeInterval);
	}

	private Future<?> flush(boolean synchronization) throws OverflowProcessorException {
		// statistic information for flush
		if (lastFlushTime > 0) {
			long thisFLushTime = System.currentTimeMillis();
			DateTime lastDateTime = new DateTime(lastFlushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			DateTime thisDateTime = new DateTime(thisFLushTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
			LOGGER.info(
					"The overflow processor {} last flush time is {}, this flush time is {}, flush time interval is {}s",
					getProcessorName(), lastDateTime, thisDateTime, (thisFLushTime - lastFlushTime) / 1000);
		}
		lastFlushTime = System.currentTimeMillis();
		// value count
		if (valueCount > 0) {
			synchronized (flushStatus) {
				while (flushStatus.isFlushing()) {
					try {
						flushStatus.wait();
					} catch (InterruptedException e) {
						LOGGER.error("Waiting the flushstate error in flush row group to store.", e);
					}
				}
			}
			try {
				// backup newIntervalFile list and emptyIntervalFileNode
				overflowFlushAction.act();
			} catch (Exception e) {
				LOGGER.error("Flush the overflow rowGroup to file faied, when overflowFlushAction act");
				throw new OverflowProcessorException(e);
			}

			if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
				try {
					WriteLogManager.getInstance().startOverflowFlush(getProcessorName());
				} catch (IOException e) {
					throw new OverflowProcessorException(e);
				}
			}
			valueCount = 0;
			// switch from work to flush
			flushStatus.setFlushing();
			switchWorkToFlush();

			if (synchronization) {
				flushOperation("synchronously");
			} else {
				flushStatus.setFlushing();
				FlushManager.getInstance().submit(new Runnable() {
					public void run() {
						flushOperation("asynchronously");
					}
				});
			}
		}
		return null;
	}

	@Override
	public boolean flush() throws IOException {
		try {
			flush(false);
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		return false;
	}

	@Override
	public void close() throws OverflowProcessorException {
		LOGGER.info("The overflow processor {} starts close operation.", getProcessorName());
		long closeStartTime = System.currentTimeMillis();
		// flush data
		flush(true);
		LOGGER.info("The overflow processor {} ends close operation.", getProcessorName());
		// log close time
		long closeEndTime = System.currentTimeMillis();
		long timeInterval = closeEndTime - closeStartTime;
		DateTime startDateTime = new DateTime(closeStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		DateTime endDateTime = new DateTime(closeStartTime, TsfileDBDescriptor.getInstance().getConfig().timeZone);
		LOGGER.info("The close operation of overflow processor {} starts at {} and ends at {}. It comsumes {}ms.",
				getProcessorName(), startDateTime, endDateTime, timeInterval);
	}

	public void clear() throws IOException {
		if (workResource != null) {
			workResource.close();
		}
		if (mergeResource != null) {
			mergeResource.close();
		}
	}

	@Override
	public boolean canBeClosed() {
		// TODO: consider merge
		return !isMerge;
	}

	@Override
	public long memoryUsage() {
		return 0;
	}

	/**
	 * @return The sum of all timeseries's metadata size within this file.
	 */
	public long getMetaSize() {
		// TODO : [MemControl] implement this
		return 0;
	}

	/**
	 * @return The size of overflow file corresponding to this processor.
	 */
	public long getFileSize() {
		File insertFile = new File(workResource.getInsertFilePath());
		File updateFile = new File(workResource.getUpdateDeleteFilePath());
		return insertFile.length() + updateFile.length() + memoryUsage();
	}

	/**
	 * Close current OverflowFile and open a new one for future writes. Block
	 * new writes and wait until current writes finish.
	 */
	private void rollToNewFile() {
		// TODO : [MemControl] implement this
	}

	/**
	 * Check whether current overflow file contains too many metadata or size of
	 * current overflow file is too large If true, close current file and open a
	 * new one.
	 */
	private boolean checkSize() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		long metaSize = getMetaSize();
		long fileSize = getFileSize();
		LOGGER.info("The overflow processor {}, the size of metadata reaches {}, the size of file reaches {}.",
				getProcessorName(), MemUtils.bytesCntToStr(metaSize), MemUtils.bytesCntToStr(fileSize));
		if (metaSize >= config.overflowMetaSizeThreshold || fileSize >= config.overflowFileSizeThreshold) {
			LOGGER.info(
					"The overflow processor {}, size({}) of the file {} reaches threshold {}, size({}) of metadata reaches threshold {}.",
					getProcessorName(), MemUtils.bytesCntToStr(fileSize), workResource.getInsertFilePath(),
					MemUtils.bytesCntToStr(config.overflowMetaSizeThreshold), MemUtils.bytesCntToStr(metaSize),
					MemUtils.bytesCntToStr(config.overflowMetaSizeThreshold));
			rollToNewFile();
			return true;
		} else {
			return false;
		}
	}
}
