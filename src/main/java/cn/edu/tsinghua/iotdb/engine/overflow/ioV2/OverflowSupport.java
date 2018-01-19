package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.util.HashMap;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class OverflowSupport {

	private Map<String, Map<String, OverflowSeriesImpl>> indexTrees;
	// private MemTable memtable;

	public OverflowSupport() {
		indexTrees = new HashMap<>();
		// init memtable
	}

	public void insert(TSRecord tsRecord) {
		// insert into memtable

	}

	public void update(String deltaObjectId, String measurementId, long startTime, long endTime, TSDataType dataType,
			byte[] value) {
		if (!indexTrees.containsKey(deltaObjectId)) {
			indexTrees.put(deltaObjectId, new HashMap<>());
		}
		if (!indexTrees.get(deltaObjectId).containsKey(measurementId)) {
			indexTrees.get(deltaObjectId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
		}
		indexTrees.get(deltaObjectId).get(measurementId).update(startTime, endTime, value);
	}

	public void delete(String deltaObjectId, String measurementId, long timestamp, TSDataType dataType) {
		if (!indexTrees.containsKey(deltaObjectId)) {
			indexTrees.put(deltaObjectId, new HashMap<>());
		}
		if (!indexTrees.get(deltaObjectId).containsKey(measurementId)) {
			indexTrees.get(deltaObjectId).put(measurementId, new OverflowSeriesImpl(measurementId, dataType));
		}
		indexTrees.get(deltaObjectId).get(measurementId).delete(timestamp);
	}

	public void queryOverflowInsert() {
		
	}

	public DynamicOneColumnData queryOverflowUpdate(String deltaObjectId, String measurementId,
			SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, TSDataType dataType, DynamicOneColumnData data) {
		if (indexTrees.containsKey(deltaObjectId)) {
			if (indexTrees.get(deltaObjectId).containsKey(measurementId)) {
				return indexTrees.get(deltaObjectId).get(measurementId).query(timeFilter, freqFilter, valueFilter,
						dataType, data);
			}
		}
		return null;
	}

	public boolean isEmptyOfOverflowSeriesMap() {
		return indexTrees.isEmpty();
	}

	public Map<String, Map<String, OverflowSeriesImpl>> getOverflowSeriesMap() {
		return indexTrees;
	}

	public boolean isEmptyOfMemTable() {
		return true;
	}

	public void getMemTabale() {

	}

	public long getMemUsage() {
		// memtable+overflowTreesMap
		return 0;
	}

	public void clear() {
		// clear overflows
		indexTrees.clear();
		// clear memtable

	}
}
