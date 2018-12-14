package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadata;
import cn.edu.tsinghua.tsfile.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.utils.Pair;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;


public class OverflowResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowResource.class);
	private String parentPath;
	private String dataPath;
	private String insertFilePath;
	private String updateDeleteFilePath;
	private String positionFilePath;
	private File insertFile;
	private File updateFile;

	private static final String insertFileName = "unseqTsFile";
	private static final String updateDeleteFileName = "overflowFile";
	private static final String positionFileName = "positionFile";
	private OverflowIO insertIO;
	// private OverflowIO updateDeleteIO;
	private static final int FOOTER_LENGTH = 4;
	private static final int POS_LENGTH = 8;

	private Map<String, Map<String, List<ChunkMetaData>>> insertMetadatas;
	//private Map<String, Map<String, List<ChunkMetaData>>> updateDeleteMetadatas;

	private List<ChunkGroupMetaData> appendInsertMetadatas;
	//private List<OFRowGroupListMetadata> appendUpdateDeleteMetadats;

	public OverflowResource(String parentPath, String dataPath) throws IOException {
		this.insertMetadatas = new HashMap<>();
		this.appendInsertMetadatas = new ArrayList<>();
		this.parentPath = parentPath;
		this.dataPath = dataPath;
		File dataFile = new File(parentPath, dataPath);
		if (!dataFile.exists()) {
			dataFile.mkdirs();
		}
		insertFile = new File(dataFile, insertFileName);
		insertFilePath = insertFile.getPath();
		updateFile = new File(dataFile, updateDeleteFileName);
		updateDeleteFilePath = updateFile.getPath();
		positionFilePath = new File(dataFile, positionFileName).getPath();
		Pair<Long, Long> position = readPositionInfo();
		try {
			// deprecated the update and delete function
			// update and delete stream
			// truncate
			// reposition
			// updateDeleteIO = new OverflowIO(new DefaultTsFileOutput(udoutputStream), false);

			// insert stream
			// truncate
			// reposition
			OverflowIO.OverflowReadWriter readWriter = new OverflowIO.OverflowReadWriter(insertFilePath);
			readWriter.wrapAsFileChannel().truncate(position.left);
			// seek to zero
			readWriter.wrapAsFileChannel().position(0);
			insertIO = new OverflowIO(readWriter);
			readMetadata();
		} catch (IOException e) {
			LOGGER.error("Failed to construct the OverflowIO.", e);
			throw e;
		}
	}

	private Pair<Long, Long> readPositionInfo() {
		try {
			FileInputStream inputStream = new FileInputStream(positionFilePath);
			byte[] insertPositionData = new byte[8];
			byte[] updatePositionData = new byte[8];
			inputStream.read(insertPositionData);
			inputStream.read(updatePositionData);
			long lastInsertPosition = BytesUtils.bytesToLong(insertPositionData);
			long lastUpdatePosition = BytesUtils.bytesToLong(updatePositionData);
			inputStream.close();
			return new Pair<Long, Long>(lastInsertPosition, lastUpdatePosition);
		} catch (IOException e) {
			long left = 0;
			long right = 0;
			File insertFile = new File(insertFilePath);
			File updateFile = new File(updateDeleteFilePath);
			if (insertFile.exists()) {
				left = insertFile.length();
			}
			if (updateFile.exists()) {
				right = updateFile.length();
			}
			return new Pair<Long, Long>(left, right);
		}
	}

	private void writePositionInfo(long lastInsertPosition, long lastUpdatePosition) throws IOException {
		FileOutputStream outputStream = new FileOutputStream(positionFilePath);
		byte[] data = new byte[16];
		BytesUtils.longToBytes(lastInsertPosition, data, 0);
		BytesUtils.longToBytes(lastUpdatePosition, data, 8);
		outputStream.write(data);
		outputStream.close();
	}

	private void readMetadata() throws IOException {
		// read update/delete meta-data
		/*updateDeleteIO.toTail();
		long position = updateDeleteIO.getPos();
		while (position != 0) {
			updateDeleteIO.getReader().seek(position - FOOTER_LENGTH);
			int metadataLength = updateDeleteIO.getReader().readInt();
			byte[] buf = new byte[metadataLength];
			updateDeleteIO.getReader().seek(position - FOOTER_LENGTH - metadataLength);
			updateDeleteIO.getReader().read(buf, 0, buf.length);
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			OFFileMetadata ofFileMetadata = new TSFileMetaDataConverter()
					.toOFFileMetadata(OverflowReadWriteThriftFormatUtils.readOFFileMetaData(bais));
			position = ofFileMetadata.getLastFooterOffset();
			for (OFRowGroupListMetadata rowGroupListMetadata : ofFileMetadata.getRowGroupLists()) {
				String deltaObjectId = rowGroupListMetadata.getDeltaObjectId();
				if (!updateDeleteMetadatas.containsKey(deltaObjectId)) {
					updateDeleteMetadatas.put(deltaObjectId, new HashMap<>());
				}
				for (OFSeriesListMetadata seriesListMetadata : rowGroupListMetadata.getSeriesList()) {
					String measurementId = seriesListMetadata.getMeasurementId();
					if (!updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
						updateDeleteMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
					}
					updateDeleteMetadatas.get(deltaObjectId).get(measurementId).addAll(0,
							seriesListMetadata.getMetaDatas());
				}
			}
		}*/
		// read insert meta-data
		insertIO.toTail();
		long position = insertIO.getPos();
		while (position != 0) {
			insertIO.getReader().position(position - FOOTER_LENGTH);
			int metadataLength = insertIO.getReader().readInt();
			byte[] buf = new byte[metadataLength];
			insertIO.getReader().position(position - FOOTER_LENGTH - metadataLength);
			insertIO.getReader().read(buf, 0, buf.length);
			ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
			TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(inputStream);
			byte[] bytesPosition = new byte[8];
			insertIO.getReader().position(position - FOOTER_LENGTH - metadataLength - POS_LENGTH);
			insertIO.getReader().read(bytesPosition, 0, POS_LENGTH);
			position = BytesUtils.bytesToLong(bytesPosition);
			for (ChunkGroupMetaData rowGroupMetaData : tsDeviceMetadata.getChunkGroups()) {
				String deltaObjectId = rowGroupMetaData.getDeviceID();
				if (!insertMetadatas.containsKey(deltaObjectId)) {
					insertMetadatas.put(deltaObjectId, new HashMap<>());
				}
				for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
					String measurementId = chunkMetaData.getMeasurementUID();
					if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
						insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
					}
					insertMetadatas.get(deltaObjectId).get(measurementId).add(0, chunkMetaData);
				}
			}
		}
	}

	public List<ChunkMetaData> getInsertMetadatas(String deltaObjectId, String measurementId,
												  TSDataType dataType) {
		List<ChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (insertMetadatas.containsKey(deltaObjectId)) {
			if (insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
				for (ChunkMetaData chunkMetaData : insertMetadatas.get(deltaObjectId).get(measurementId)) {
					// filter
					if (chunkMetaData.getTsDataType().equals(dataType)) {
						chunkMetaDatas.add(chunkMetaData);
					}
				}
			}
		}
		return chunkMetaDatas;
	}

	@Deprecated
	public List<ChunkMetaData> getUpdateDeleteMetadatas(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		/*List<ChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (updateDeleteMetadatas.containsKey(deltaObjectId)) {
			if (updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
				for (ChunkMetaData chunkMetaData : updateDeleteMetadatas.get(deltaObjectId)
						.get(measurementId)) {
					// filter
					if (chunkMetaData.getTsDataType().equals(dataType)) {
						chunkMetaDatas.add(chunkMetaData);
					}
				}
			}
		}*/
		return null;
	}

	public void flush(FileSchema fileSchema, IMemTable memTable,
					  Map<String, Map<String, OverflowSeriesImpl>> overflowTrees, String processorName) throws IOException {
		// insert data
		long startPos = insertIO.getPos();
		long startTime = System.currentTimeMillis();
		flush(fileSchema, memTable);
		long timeInterval = System.currentTimeMillis() - startTime;
		timeInterval = timeInterval == 0 ? 1 : timeInterval;
		long insertSize = insertIO.getPos() - startPos;
		LOGGER.info(
				"Overflow processor {} flushes overflow insert data, actual:{}, time consumption:{} ms, flush rate:{}/s",
				processorName, MemUtils.bytesCntToStr(insertSize), timeInterval,
				MemUtils.bytesCntToStr(insertSize / timeInterval * 1000));
		writePositionInfo(insertIO.getPos(), 0);
	}

	public void flush(FileSchema fileSchema, IMemTable memTable) throws IOException {
		if (memTable != null && !memTable.isEmpty()) {
			insertIO.toTail();
			long lastPosition = insertIO.getPos();
			MemTableFlushUtil.flushMemTable(fileSchema, insertIO, memTable);
			List<ChunkGroupMetaData> rowGroupMetaDatas = insertIO.getChunkGroupMetaDatas();
			appendInsertMetadatas.addAll(rowGroupMetaDatas);
			if (!rowGroupMetaDatas.isEmpty()) {
				insertIO.getWriter().write(BytesUtils.longToBytes(lastPosition));
				TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
				tsDeviceMetadata.setChunkGroupMetadataList(rowGroupMetaDatas);
				long start = insertIO.getPos();
				tsDeviceMetadata.serializeTo(insertIO.getWriter());
				long end = insertIO.getPos();
				insertIO.getWriter().write(BytesUtils.intToBytes((int) (end - start)));
				// clear the meta-data of insert IO
				insertIO.clearRowGroupMetadatas();
			}
		}
	}

	@Deprecated
	public void flush(Map<String, Map<String, OverflowSeriesImpl>> overflowTrees) throws IOException {
		/*if (overflowTrees != null && !overflowTrees.isEmpty()) {
			updateDeleteIO.toTail();
			long lastPosition = updateDeleteIO.getPos();
			List<OFRowGroupListMetadata> ofRowGroupListMetadatas = updateDeleteIO.flush(overflowTrees);
			appendUpdateDeleteMetadats.addAll(ofRowGroupListMetadatas);
			OFFileMetadata ofFileMetadata = new OFFileMetadata(lastPosition, ofRowGroupListMetadatas);
			TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
			long start = updateDeleteIO.getPos();
			OverflowReadWriteThriftFormatUtils.writeOFFileMetaData(
					metadataConverter.toThriftOFFileMetadata(0, ofFileMetadata), updateDeleteIO.getWriter());
			long end = updateDeleteIO.getPos();
			updateDeleteIO.getWriter().write(BytesUtils.intToBytes((int) (end - start)));
		}*/
	}

	public void appendMetadatas() {
		if (!appendInsertMetadatas.isEmpty()) {
			for (ChunkGroupMetaData rowGroupMetaData : appendInsertMetadatas) {
				for (ChunkMetaData seriesChunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
					addInsertMetadata(rowGroupMetaData.getDeviceID(),
							seriesChunkMetaData.getMeasurementUID(), seriesChunkMetaData);
				}
			}
			appendInsertMetadatas.clear();
		}
		/*if (!appendUpdateDeleteMetadats.isEmpty()) {
			for (OFRowGroupListMetadata ofRowGroupListMetadata : appendUpdateDeleteMetadats) {
				String deltaObjectId = ofRowGroupListMetadata.getDeltaObjectId();
				for (OFSeriesListMetadata ofSeriesListMetadata : ofRowGroupListMetadata.getSeriesLists()) {
					String measurementId = ofSeriesListMetadata.getMeasurementId();
					for (ChunkMetaData chunkMetaData : ofSeriesListMetadata.getMetaDatas()) {
						addUpdateDeletetMetadata(deltaObjectId, measurementId, chunkMetaData);
					}
				}
			}
			appendUpdateDeleteMetadats.clear();
		}*/
	}

	public String getInsertFilePath() {
		return insertFilePath;
	}

	public File getInsertFile() {
		return insertFile;
	}

	public String getPositionFilePath() {
		return positionFilePath;
	}

	public String getUpdateDeleteFilePath() {
		return updateDeleteFilePath;
	}

	public File getUpdateDeleteFile() {
		return updateFile;
	}

	public void close() throws IOException {
		insertMetadatas.clear();
		//updateDeleteMetadatas.clear();
		insertIO.close();
		//updateDeleteIO.close();
	}

	public void deleteResource() throws IOException {
		// cleanDir(new File(parentPath, dataPath).getPath());
		FileUtils.forceDelete(new File(parentPath, dataPath));
	}

	private void cleanDir(String dir) throws IOException {
		File file = new File(dir);
		if (file.exists()) {
			if (file.isDirectory()) {
				for (File subFile : file.listFiles()) {
					cleanDir(subFile.getAbsolutePath());
				}
			}
			if (!file.delete()) {
				throw new IOException(String.format("The file %s can't be deleted", dir));
			}
		}
	}

	private void addInsertMetadata(String deltaObjectId, String measurementId, ChunkMetaData chunkMetaData) {
		if (!insertMetadatas.containsKey(deltaObjectId)) {
			insertMetadatas.put(deltaObjectId, new HashMap<>());
		}
		if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
			insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
		}
		insertMetadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
	}

	@Deprecated
	private void addUpdateDeletetMetadata(String deltaObjectId, String measurementId){
		/*	ChunkMetaData chunkMetaData) {
		if (!updateDeleteMetadatas.containsKey(deltaObjectId)) {
			updateDeleteMetadatas.put(deltaObjectId, new HashMap<>());
		}
		if (!updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
			updateDeleteMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
		}
		updateDeleteMetadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);*/
	}
}