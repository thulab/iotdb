package cn.edu.tsinghua.iotdb.engine.bufferwriteV2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class BufferWriteResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteResource.class);
	private static final String restoreSuffix = ".restore";

	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metadatas;
	private BufferWriteIO bufferWriteIO;
	private String insertFilePath;
	private String restoreFilePath;

	public BufferWriteResource(String dataPath, String insertFilePath) throws IOException {
		this.insertFilePath = insertFilePath;
		this.restoreFilePath = insertFilePath + restoreSuffix;
		metadatas = new HashMap<>();

		recover();
		// check status and restore
		// restore the metadatas
		// restore IO
	}

	private void recover() throws IOException {
		File insertFile = new File(insertFilePath);
		File restoreFile = new File(restoreFilePath);
		if (insertFile.exists() && restoreFile.exists()) {

		} else {
			insertFile.delete();
			restoreFile.delete();
			bufferWriteIO = new BufferWriteIO(new TsRandomAccessFileWriter(insertFile), 0, new ArrayList<>());
		}
	}

	private void writeRestoreInfo() throws IOException {

		long position = bufferWriteIO.getPos();
		List<RowGroupMetaData> append = bufferWriteIO.getAppendedRowGroupMetadata();
		TsRowGroupBlockMetaData blockMetaData = new TsRowGroupBlockMetaData(append);

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

	public void flush(FileSchema fileSchema, IMemTable iMemTable) {

	}

	public void close() {

	}

	private void delete() {

	}
}
