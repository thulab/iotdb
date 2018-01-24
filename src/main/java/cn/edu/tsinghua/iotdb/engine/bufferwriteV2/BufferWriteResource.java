package cn.edu.tsinghua.iotdb.engine.bufferwriteV2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class BufferWriteResource {

	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteResource.class);
	private static final int TSMETADATABYTESIZE = 4;
	private static final int TSFILEPOINTBYTESIZE = 8;

	private static final String restoreSuffix = ".restore";
	private static final String DEFAULT_MODE = "rw";
	private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metadatas;
	private BufferWriteIO bufferWriteIO;
	private String insertFilePath;
	private String restoreFilePath;
	private String processorName;

	public BufferWriteResource(String processorName, String insertFilePath) throws IOException {
		this.insertFilePath = insertFilePath;
		this.restoreFilePath = insertFilePath + restoreSuffix;
		this.processorName = processorName;
		this.metadatas = new HashMap<>();
		recover();
	}

	private void recover() throws IOException {
		File insertFile = new File(insertFilePath);
		File restoreFile = new File(restoreFilePath);
		if (insertFile.exists() && restoreFile.exists()) {
			// read restore file
			Pair<Long, List<RowGroupMetaData>> restoreInfo = readRestoreInfo();
			long position = restoreInfo.left;
			List<RowGroupMetaData> metadatas = restoreInfo.right;
			// cut off tsfile
			cutOffFile(position);
			// recovery the BufferWriteIO
			bufferWriteIO = new BufferWriteIO(new TsRandomAccessFileWriter(insertFile), position, metadatas);
			recoverMetadata(metadatas);
			LOGGER.info(
					"Recover the bufferwrite processor {}, the tsfile path is {}, the position of last flush is {}, the size of rowGroupMetadata is {}",
					processorName, insertFilePath, restoreFile, position, metadatas.size());
		} else {
			insertFile.delete();
			restoreFile.delete();
			bufferWriteIO = new BufferWriteIO(new TsRandomAccessFileWriter(insertFile), 0, new ArrayList<>());
			writeRestoreInfo();
		}
	}

	private void recoverMetadata(List<RowGroupMetaData> rowGroupMetaDatas) {
		for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
			String deltaObjectId = rowGroupMetaData.getDeltaObjectID();
			if (!metadatas.containsKey(deltaObjectId)) {
				metadatas.put(deltaObjectId, new HashMap<>());
			}
			for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
				String measurementId = chunkMetaData.getProperties().getMeasurementUID();
				if (!metadatas.get(deltaObjectId).containsKey(measurementId)) {
					metadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
				}
				metadatas.get(deltaObjectId).get(measurementId).add(chunkMetaData);
			}
		}
	}

	private void cutOffFile(long length) throws IOException {
		String tempPath = insertFilePath + ".backup";
		File tempFile = new File(tempPath);
		File normalFile = new File(insertFilePath);
		if (normalFile.exists() && normalFile.length() > 0) {
			RandomAccessFile normalReader = null;
			RandomAccessFile tempWriter = null;
			try {
				normalReader = new RandomAccessFile(normalFile, "r");
				tempWriter = new RandomAccessFile(tempFile, "rw");
			} catch (FileNotFoundException e) {
				LOGGER.error("Failed to cut off the file {}.", insertFilePath, e);
				if (normalReader != null) {
					normalReader.close();
				}
				if (tempWriter != null) {
					tempWriter.close();
				}
				throw e;
			}

			if (tempFile.exists()) {
				tempFile.delete();
			}
			long offset = 0;
			int step = 4 * 1024 * 1024;
			byte[] buff = new byte[step];
			while (length - offset >= step) {
				try {
					normalReader.readFully(buff);
					tempWriter.write(buff);
				} catch (IOException e) {
					LOGGER.error("Failed to read data, the file is {}.", insertFilePath, e);
					throw e;
				}
				offset = offset + step;
			}
			normalReader.readFully(buff, 0, (int) (length - offset));
			tempWriter.write(buff, 0, (int) (length - offset));
			normalReader.close();
			tempWriter.close();
		}
		normalFile.delete();
		tempFile.renameTo(normalFile);
	}

	private void writeRestoreInfo() throws IOException {
		long lastPosition;
		lastPosition = bufferWriteIO.getPos();
		List<RowGroupMetaData> appendRowGroupMetaDatas = bufferWriteIO.getAppendedRowGroupMetadata();
		TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
		tsRowGroupBlockMetaData.setRowGroups(appendRowGroupMetaDatas);
		RandomAccessFile out = null;
		out = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
		try {
			if (out.length() > 0) {
				out.seek(out.length() - TSFILEPOINTBYTESIZE);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(tsRowGroupBlockMetaData.convertToThrift(), baos);
			// write metadata size using int
			int metadataSize = baos.size();
			out.write(BytesUtils.intToBytes(metadataSize));
			// write metadata
			out.write(baos.toByteArray());
			// write tsfile position using byte[8] which is present one long
			// number
			byte[] lastPositionBytes = BytesUtils.longToBytes(lastPosition);
			out.write(lastPositionBytes);
		} finally {
			out.close();
		}
	}

	private Pair<Long, List<RowGroupMetaData>> readRestoreInfo() throws IOException {
		byte[] lastPostionBytes = new byte[TSFILEPOINTBYTESIZE];
		List<RowGroupMetaData> groupMetaDatas = new ArrayList<>();
		RandomAccessFile randomAccessFile = null;
		randomAccessFile = new RandomAccessFile(restoreFilePath, "rw");
		try {
			long fileLength = randomAccessFile.length();
			// read tsfile position
			long point = randomAccessFile.getFilePointer();
			while (point + TSFILEPOINTBYTESIZE < fileLength) {
				byte[] metadataSizeBytes = new byte[TSMETADATABYTESIZE];
				randomAccessFile.read(metadataSizeBytes);
				int metadataSize = BytesUtils.bytesToInt(metadataSizeBytes);
				byte[] thriftBytes = new byte[metadataSize];
				randomAccessFile.read(thriftBytes);
				ByteArrayInputStream inputStream = new ByteArrayInputStream(thriftBytes);
				RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
						.readRowGroupBlockMetaData(inputStream);
				TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
				blockMeta.convertToTSF(rowGroupBlockMetaData);
				groupMetaDatas.addAll(blockMeta.getRowGroups());
				point = randomAccessFile.getFilePointer();
			}
			// read the tsfile position information using byte[8] which is
			// present one long number.
			randomAccessFile.read(lastPostionBytes);
			long lastPostion = BytesUtils.bytesToLong(lastPostionBytes);
			Pair<Long, List<RowGroupMetaData>> result = new Pair<Long, List<RowGroupMetaData>>(lastPostion,
					groupMetaDatas);
			return result;
		} finally {
			randomAccessFile.close();
		}
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

	public String getInsertFilePath() {
		return insertFilePath;
	}

	public String getRestoreFilePath() {
		return restoreFilePath;
	}

	public void flush(FileSchema fileSchema, IMemTable iMemTable) {
		// use the memtable flush funtion
		
		// get metadata

		// add metadata to map

		// flush metadata to restore file
	}

	public void close() {
		// call flush

		// close the file and delete the restore file
	}
}
