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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowReadWriteThriftFormatUtils;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class OverflowResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowResource.class);
	private String parentPath;
	private String dataPath;
	private String insertFilePath;
	private String updateDeleteFilePath;
	private String positionFilePath;

	private String insertFileName = "unseqTsFile";
	private String updateDeleteFileName = "overflowFile";
	private String positionFileName = "positionFile";
	private OverflowIO insertIO;
	private OverflowIO updateDeleteIO;
	private static final int FOOTER_LENGTH = 4;
	private static final int POS_LENGTH = 8;

	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> insertMetadatas;
	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> updateDeleteMetadatas;

	public OverflowResource(String parentPath, String dataPath) {
		this.insertMetadatas = new HashMap<>();
		this.updateDeleteMetadatas = new HashMap<>();
		this.parentPath = parentPath;
		this.dataPath = dataPath;
		File dataFile = new File(new File(parentPath), dataPath);
		if (!dataFile.exists()) {
			dataFile.mkdirs();
		}
		insertFilePath = new File(dataFile, insertFileName).getPath();
		updateDeleteFilePath = new File(dataFile, updateDeleteFileName).getPath();
		positionFilePath = new File(dataFile, positionFileName).getPath();
		Pair<Long, Long> position = readPositionInfo();
		try {
			updateDeleteIO = new OverflowIO(updateDeleteFilePath, position.right, false);
			insertIO = new OverflowIO(insertFilePath, position.left, true);
			readMetadata();
		} catch (IOException e) {
			LOGGER.error("Failed to construct the OverflowIO.", e);
			e.printStackTrace();
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
		updateDeleteIO.toTail();
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
				for (OFSeriesListMetadata seriesListMetadata : rowGroupListMetadata.getSeriesLists()) {
					String measurementId = seriesListMetadata.getMeasurementId();
					if (!updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
						updateDeleteMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
					}
					updateDeleteMetadatas.get(deltaObjectId).get(measurementId).addAll(0,
							seriesListMetadata.getMetaDatas());
				}
			}
		}
		// read insert meta-data
		insertIO.toTail();
		position = insertIO.getPos();
		// TODD: attention
		while (position != 0) {
			insertIO.getReader().seek(position - FOOTER_LENGTH);
			int metadataLength = insertIO.getReader().readInt();
			byte[] buf = new byte[metadataLength];
			insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength);
			insertIO.getReader().read(buf, 0, buf.length);
			ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
			RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
					.readRowGroupBlockMetaData(inputStream);
			TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
			blockMeta.convertToTSF(rowGroupBlockMetaData);
			byte[] bytesPosition = new byte[8];
			insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength - POS_LENGTH);
			insertIO.getReader().read(bytesPosition, 0, POS_LENGTH);
			position = BytesUtils.bytesToLong(bytesPosition);
			for (RowGroupMetaData rowGroupMetaData : blockMeta.getRowGroups()) {
				String deltaObjectId = rowGroupMetaData.getDeltaObjectID();
				if (!insertMetadatas.containsKey(deltaObjectId)) {
					insertMetadatas.put(deltaObjectId, new HashMap<>());
				}
				for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
					String measurementId = chunkMetaData.getProperties().getMeasurementUID();
					if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
						insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
					}
					insertMetadatas.get(deltaObjectId).get(measurementId).add(0, chunkMetaData);
				}
			}
		}
	}

	public List<TimeSeriesChunkMetaData> getInsertMetadatas(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (insertMetadatas.containsKey(deltaObjectId)) {
			if (insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
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

	public List<TimeSeriesChunkMetaData> getUpdateDeleteMetadatas(String deltaObjectId, String measurementId,
			TSDataType dataType) {
		List<TimeSeriesChunkMetaData> chunkMetaDatas = new ArrayList<>();
		if (updateDeleteMetadatas.containsKey(deltaObjectId)) {
			if (updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
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

	public void flush(FileSchema fileSchema, IMemTable memTable,
			Map<String, Map<String, OverflowSeriesImpl>> overflowTrees) throws IOException {
		flush(fileSchema, memTable);
		flush(overflowTrees);
		writePositionInfo(insertIO.getPos(), updateDeleteIO.getPos());
	}

	public void flush(FileSchema fileSchema, IMemTable memTable) throws IOException {
		if (memTable != null && !memTable.isEmpty()) {
			// memtable
			insertIO.toTail();
			long lastPosition = insertIO.getPos();
			// TODO file-schema
			// TODO page size
			MemTableFlushUtil.flushMemTable(fileSchema, insertIO, memTable, 1024 * 1024);
			List<RowGroupMetaData> rowGroupMetaDatas = insertIO.getRowGroups();
			for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
				for (TimeSeriesChunkMetaData seriesChunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
					addInsertMetadata(rowGroupMetaData.getDeltaObjectID(),
							seriesChunkMetaData.getProperties().getMeasurementUID(), seriesChunkMetaData);
				}
			}
			if (!rowGroupMetaDatas.isEmpty()) {
				insertIO.getWriter().write(BytesUtils.longToBytes(lastPosition));
				TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData(rowGroupMetaDatas);
				long start = insertIO.getPos();
				ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(tsRowGroupBlockMetaData.convertToThrift(),
						insertIO.getWriter());
				long end = insertIO.getPos();
				insertIO.getWriter().write(BytesUtils.intToBytes((int) (end - start)));
				insertIO.clearRowGroupMetadatas();
			}
		}
	}

	public void flush(Map<String, Map<String, OverflowSeriesImpl>> overflowTrees) throws IOException {
		if (overflowTrees != null && !overflowTrees.isEmpty()) {
			updateDeleteIO.toTail();
			long lastPosition = updateDeleteIO.getPos();
			List<OFRowGroupListMetadata> ofRowGroupListMetadatas = updateDeleteIO.flush(overflowTrees);
			OFFileMetadata ofFileMetadata = new OFFileMetadata(lastPosition, ofRowGroupListMetadatas);
			TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
			long start = updateDeleteIO.getPos();
			OverflowReadWriteThriftFormatUtils.writeOFFileMetaData(
					metadataConverter.toThriftOFFileMetadata(0, ofFileMetadata), updateDeleteIO.getWriter());
			long end = updateDeleteIO.getPos();
			updateDeleteIO.getWriter().write(BytesUtils.intToBytes((int) (end - start)));
			for (OFRowGroupListMetadata ofRowGroupListMetadata : ofRowGroupListMetadatas) {
				String deltaObjectId = ofRowGroupListMetadata.getDeltaObjectId();
				if (!updateDeleteMetadatas.containsKey(deltaObjectId)) {
					updateDeleteMetadatas.put(deltaObjectId, new HashMap<>());
				}
				for (OFSeriesListMetadata ofSeriesListMetadata : ofRowGroupListMetadata.getSeriesLists()) {
					String measurementId = ofSeriesListMetadata.getMeasurementId();
					if (!updateDeleteMetadatas.get(deltaObjectId).containsKey(measurementId)) {
						updateDeleteMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
					}
					updateDeleteMetadatas.get(deltaObjectId).get(measurementId)
							.addAll(ofSeriesListMetadata.getMetaDatas());
				}
			}
		}
	}

	public String getInsertFilePath() {
		return insertFilePath;
	}

	public String getUpdateDeleteFilePath() {
		return updateDeleteFilePath;
	}

	public void close() throws IOException {
		insertMetadatas.clear();
		updateDeleteMetadatas.clear();
		insertIO.close();
		updateDeleteIO.close();
	}

	public void deleteResource() {
		new File(insertFilePath).delete();
		new File(updateDeleteFilePath).delete();
		new File(positionFilePath).delete();
		new File(parentPath, dataPath).delete();
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

}
