package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.utils.FlushState;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class OverflowProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowProcessor.class);
	private OverflowResource workResource;
	private OverflowResource mergeResource;

	private OverflowSupport workSupport;
	private OverflowSupport flushSupport;
	private OverflowSupport mergeSupport;

	private FlushState flushStatus;
	private boolean isMerge;
	private String processorName;
	private ReentrantLock queryFlushLock = new ReentrantLock();
	private final TsfileDBConfig dbConfig = TsfileDBDescriptor.getInstance().getConfig();

	public OverflowProcessor() {
		// check merge or work status
	}

	public void insert(TSRecord tsRecord) {
		workSupport.insert(tsRecord);
	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType type,
			byte[] value) {
		workSupport.update(deltaObjectId, measurementId, startTime, endTime, type, value);
	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType type) {
		workSupport.delete(deltaObjectId, measurementId, timestamp, type);
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
			queryMergeDataInOverflowInsert(deltaObjectId, measurementId, dataType);
			// query update/delete data in memory and overflowFiles
			DynamicOneColumnData updateDataInMem = queryOverflowUpdateInMemory(deltaObjectId, measurementId, timeFilter,
					freqFilter, valueFilter, dataType);
			queryWorkDataInOverflowUpdate(deltaObjectId, measurementId, dataType);
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

		if (flushStatus.isFlushing()) {

		}

	}

	private DynamicOneColumnData queryOverflowUpdateInMemory(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType) {
		DynamicOneColumnData columnData = workSupport.queryOverflowUpdate(deltaObjectId, measurementId, timeFilter,
				freqFilter, valueFilter, dataType, null);
		if (flushStatus.isFlushing()) {
			columnData = flushSupport.queryOverflowUpdate(deltaObjectId, measurementId, timeFilter, freqFilter,
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

	}

	private void switchFlushToWork() {

	}

	public void switchWorkToMerge() {

	}

	public void switchMergeToWork() {

	}

	public boolean isMerge() {
		return isMerge;
	}

	public boolean isFlush() {
		synchronized (flushStatus) {
			return flushStatus.isFlushing();
		}
	}

	private void flush(boolean synchronization) {

	}

	public void flush() {
		// flush interval

		//
	}

	public void close() {

	}

	public long getMemUsage() {
		return 0;
	}
}
