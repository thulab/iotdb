package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class BufferWriteResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteResource.class);
	private static final String restoreSuffix = ".restore";

	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metadatas;
	private BufferWriteIOWriter bufferWriteIOWriter;
	private String insertFilePath;
	private String restoreFilePath;

	public BufferWriteResource(String insertFilePath) {
		this.insertFilePath = insertFilePath;
		this.restoreFilePath = insertFilePath + restoreSuffix;
		metadatas = new HashMap<>();
		// check status and restore

	}

	private void writeRestoreInfo() {

	}

	private void readRestoreInfo() {

	}

	public List<TimeSeriesChunkMetaData> getInsertMetadatas(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (metadatas.containsKey(deltaObjectId)) {
			if (metadatas.get(deltaObjectId).containsKey(measurementId)) {
				for (TimeSeriesChunkMetaData chunkMetaData : metadatas.get(deltaObjectId).get(measurementId)) {
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
		if (!metadatas.containsKey(deltaObjectId)) {
			metadatas.put(deltaObjectId, new HashMap<>());
		}
		if (!metadatas.get(deltaObjectId).containsKey(measurementId)) {
			metadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
		}
		metadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
	}

	public void flush() {

	}

	public void close() {

	}

	private void delete() {
		
	}
}
