package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class OverflowResource {

	private String insertFilePath;
	private String updateDeleteFilePath;

	private OverflowIO insertIO;
	private OverflowIO updateDeleteIO;

	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> insertMetadatas;
	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> updateDeleteMetadatas;

	public OverflowResource() {
		insertMetadatas = new HashMap<>();
		updateDeleteMetadatas = new HashMap<>();
	}

	public List<TimeSeriesChunkMetaData> getInsertMetadatas(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (insertMetadatas.containsKey(deltaObjectId)) {
			if (insertMetadatas.containsKey(measurementId)) {
				for (TimeSeriesChunkMetaData chunkMetaData : insertMetadatas.get(deltaObjectId).get(measurementId)) {
					// filter
					if (dataType.equals(chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType())) {
						chunkMetaDatas.add(chunkMetaData);
					}
				}
			}
		}
		return chunkMetaDatas;
	}

	public void addInsertMetadata(String deltaObjectId, String measurementId, TimeSeriesChunkMetaData chunkMetaData) {
		if (!insertMetadatas.containsKey(deltaObjectId)) {
			insertMetadatas.put(deltaObjectId, new HashMap<>());
		}
		if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
			insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
		}
		insertMetadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
	}

	public List<TimeSeriesChunkMetaData> getUpdateDeleteMetadatas(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (updateDeleteMetadatas.containsKey(deltaObjectId)) {
			if (updateDeleteMetadatas.containsKey(measurementId)) {
				for (TimeSeriesChunkMetaData chunkMetaData : updateDeleteMetadatas.get(deltaObjectId)
						.get(measurementId)) {
					// filter
					if (dataType.equals(chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType())) {
						chunkMetaDatas.add(chunkMetaData);
					}
				}
			}
		}
		return chunkMetaDatas;
	}

	public void addUpdateDeletetMetadata(String deltaObjectId, String measurementId,
			TimeSeriesChunkMetaData chunkMetaData) {
		if (!updateDeleteMetadatas.containsKey(deltaObjectId)) {
			updateDeleteMetadatas.put(deltaObjectId, new HashMap<>());
		}
		if (!updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
			updateDeleteMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
		}
		updateDeleteMetadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
	}

	public String getInsertFilePath() {
		return insertFilePath;
	}

	public String getUpdateDeleteFilePath() {
		return updateDeleteFilePath;
	}

	public OverflowIO getInsertIO() {
		return insertIO;
	}

	public OverflowIO getUpdateDeleteIO() {
		return updateDeleteIO;
	}

	public void close() throws IOException {
		insertMetadatas.clear();
		updateDeleteMetadatas.clear();
		insertIO.close();
		updateDeleteIO.close();
	}

}
