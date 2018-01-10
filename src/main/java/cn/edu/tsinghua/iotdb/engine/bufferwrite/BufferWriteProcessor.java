package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.Processor;
import cn.edu.tsinghua.iotdb.engine.flushthread.FlushManager;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.engine.utils.FlushState;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.newwritelog.lognodemanager.MultiFileNodeManager;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.WriteLogNode;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.series.IRowGroupWriter;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferWriteProcessor extends Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(BufferWriteProcessor.class);
	private static final TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private static final TsfileDBConfig TsFileDBConf = TsfileDBDescriptor.getInstance().getConfig();
	private static final MManager mManager = MManager.getInstance();
	private static final int TSMETADATABYTESIZE = 4;
	private static final int TSFILEPOINTBYTESIZE = 8;

	private boolean isFlushingSync = false;
	private volatile FlushState flushState = new FlushState();
	private ReadWriteLock convertBufferLock = new ReentrantReadWriteLock(false);

	private FileSchema fileSchema;
	private BufferWriteIOWriter bufferIOWriter;
	private BufferWriteRecordWriter recordWriter;
	private int lastRowgroupSize = 0;

	// this just the bufferwrite file name
	private String fileName;
	private static final String restoreFile = ".restore";
	// this is the bufferwrite file absolute path
	private String bufferwriteRestoreFilePath;
	private String bufferwriteOutputFilePath;
	private String bufferwriterelativePath;

	private boolean isNewProcessor = false;

	private Action bufferwriteFlushAction = null;
	private Action bufferwriteCloseAction = null;
	private Action filenodeFlushAction = null;

	private long memUsed = 0;

    private WriteLogNode logNode;
    private String processorStoreFileName;

	public BufferWriteProcessor(String processorName, String fileName, Map<String, Object> parameters)
			throws BufferWriteProcessorException {
		super(processorName);

		this.fileName = fileName;
		String restoreFileName = fileName + restoreFile;

		String bufferwriteDirPath = TsFileDBConf.bufferWriteDir;
		if (bufferwriteDirPath.length() > 0
				&& bufferwriteDirPath.charAt(bufferwriteDirPath.length() - 1) != File.separatorChar) {
			bufferwriteDirPath = bufferwriteDirPath + File.separatorChar;
		}
		String dataDirPath = bufferwriteDirPath + processorName;
		File dataDir = new File(dataDirPath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
			LOGGER.info("The bufferwrite processor data dir doesn't exists, and mkdir the dir {}.", dataDirPath);
		}
		File outputFile = new File(dataDir, fileName);
		File restoreFile = new File(dataDir, restoreFileName);
		bufferwriteRestoreFilePath = restoreFile.getPath();
		bufferwriteOutputFilePath = outputFile.getPath();
		bufferwriterelativePath = processorName + File.separatorChar + fileName;
		// get the fileschema
		try {
			fileSchema = constructFileSchema(processorName);
		} catch (PathErrorException | WriteProcessException e) {
			LOGGER.error("Get the FileSchema error, the bufferwrite is {}.", processorName);
			throw new BufferWriteProcessorException(e);
		}
		//
		// There is one damaged file, and the restoreFile exist
		//
		if (outputFile.exists() && restoreFile.exists()) {

			bufferwriteRecovery();

		} else {

			ITsRandomAccessFileWriter outputWriter;
			try {
				outputWriter = new TsRandomAccessFileWriter(outputFile);
			} catch (IOException e) {
				LOGGER.error("Construct the TSRandomAccessFileWriter error, the absolutePath is {}.",
						outputFile.getPath());
				throw new BufferWriteProcessorException(e);
			}

			try {
				bufferIOWriter = new BufferWriteIOWriter(outputWriter);
			} catch (IOException e) {
				LOGGER.error("Get the BufferWriteIOWriter error, the bufferwrite is {}.", processorName);
				throw new BufferWriteProcessorException(e);
			}

			try {
				recordWriter = new BufferWriteRecordWriter(TsFileConf, bufferIOWriter, fileSchema);
			} catch (WriteProcessException e) {
				throw new BufferWriteProcessorException(e);
			}
			isNewProcessor = true;
			// write restore file
			writeStoreToDisk();
		}
		// init action
		// the action from the corresponding filenode processor
		bufferwriteFlushAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION);
		bufferwriteCloseAction = (Action) parameters.get(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION);
		filenodeFlushAction = (Action) parameters.get(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION);

		try {
			logNode = MultiFileNodeManager.getInstance().getNode(getProcessorName() + "-bufferwrite", restoreFileName, processorStoreFileName);
		} catch (IOException e) {
			LOGGER.error("Cannot create wal node for bufferwrite processor {}",processorName);
			throw new BufferWriteProcessorException(e);
		}
	}

    public WriteLogNode getLogNode() {
        return logNode;
    }

	/**
	 * <p>
	 * Recovery the bufferwrite status.<br>
	 * The one part is the last intervalFile<br>
	 * The other part is all the intervalFile, and other file will be deleted
	 * </p>
	 *
	 * @throws BufferWriteProcessorException
	 */
	private void bufferwriteRecovery() throws BufferWriteProcessorException {

		Pair<Long, List<RowGroupMetaData>> pair;
		try {
			pair = ReadStoreFromDisk();
		} catch (IOException e) {
			LOGGER.error("Read bufferwrite {} restore file failed.", getProcessorName());
			throw new BufferWriteProcessorException(e);
		}
		ITsRandomAccessFileWriter output;
		long lastPosition = pair.left;
		File lastBufferWriteFile = new File(bufferwriteOutputFilePath);
		if (lastBufferWriteFile.length() != lastPosition) {
			LOGGER.warn("The length of the last bufferwrite file is {}, the lastPosion is {}.",
					lastBufferWriteFile.length(), lastPosition);
			try {
				cutOffFile(lastPosition);
			} catch (IOException e) {
				LOGGER.error(
						"Cut off damaged file error. the damaged file path is {}, the length is {}, the cut off length is {}.",
						bufferwriteOutputFilePath, lastBufferWriteFile.length(), lastPosition);
				throw new BufferWriteProcessorException(e);
			}
		}
		try {
			// Notice: the offset is seek to end of the file by API of kr
			output = new TsRandomAccessFileWriter(lastBufferWriteFile);
		} catch (IOException e) {
			LOGGER.error("Can't construct the RandomAccessOutputStream, the outputPath is {}.",
					bufferwriteOutputFilePath);
			throw new BufferWriteProcessorException(e);
		}
		try {
			// Notice: the parameter of lastPosition is not used beacuse of the
			// API of kr
			bufferIOWriter = new BufferWriteIOWriter(output, lastPosition, pair.right);
		} catch (IOException e) {
			LOGGER.error("Can't get the bufferwrite io when recovery, the bufferwrite is {}.", getProcessorName());
			throw new BufferWriteProcessorException(e);
		}
		try {
			recordWriter = new BufferWriteRecordWriter(TsFileConf, bufferIOWriter, fileSchema);
		} catch (WriteProcessException e) {
			throw new BufferWriteProcessorException(e);
		}
		isNewProcessor = false;
	}

	private void cutOffFile(long length) throws IOException {

		String tempPath = bufferwriteOutputFilePath + ".backup";
		File tempFile = new File(tempPath);
		File normalFile = new File(bufferwriteOutputFilePath);

		if (normalFile.exists() && normalFile.length() > 0) {

			RandomAccessFile normalReader = null;
			RandomAccessFile tempWriter = null;
			try {
				normalReader = new RandomAccessFile(normalFile, "r");
				tempWriter = new RandomAccessFile(tempFile, "rw");
			} catch (FileNotFoundException e) {
				LOGGER.error(
						"Can't get the RandomAccessFile read and write, the normal path is {}, the temp path is {}.",
						bufferwriteOutputFilePath, tempPath);
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
					LOGGER.error("normalReader read data failed or tempWriter write data error.");
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

	/**
	 * This is only used after flush one rowroup data successfully.
	 *
	 * @throws BufferWriteProcessorException
	 */
	private void writeStoreToDisk() throws BufferWriteProcessorException {

		long lastPosition;
		try {
			lastPosition = bufferIOWriter.getPos();
		} catch (IOException e) {
			LOGGER.error("Can't get the bufferwrite io position.");
			throw new BufferWriteProcessorException(e);
		}
		List<RowGroupMetaData> rowGroupMetaDatas = bufferIOWriter.getRowGroups();
		List<RowGroupMetaData> appendMetadata = new ArrayList<>();
		for (int i = lastRowgroupSize; i < rowGroupMetaDatas.size(); i++) {
			appendMetadata.add(rowGroupMetaDatas.get(i));
		}
		lastRowgroupSize = rowGroupMetaDatas.size();
		TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
		tsRowGroupBlockMetaData.setRowGroups(appendMetadata);

		RandomAccessFile out = null;
		try {
			out = new RandomAccessFile(bufferwriteRestoreFilePath, "rw");
		} catch (FileNotFoundException e) {
			LOGGER.error("The restore file can't be created, the file path is {}.", bufferwriteRestoreFilePath);
			throw new BufferWriteProcessorException(e);
		}
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
			LOGGER.info("Bufferwrite {} write restore information to the restore file.", getProcessorName());
		} catch (IOException e) {
			LOGGER.error("Serialize the TSFileMetaData error.");
			throw new BufferWriteProcessorException(e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new BufferWriteProcessorException(e);
				}
			}
		}
	}

	/**
	 * This is used to delete the file which is used to restore buffer write
	 * processor. This is only used after closing the buffer write processor
	 * successfully.
	 */
	private void deleteRestoreFile() {
		File restoreFile = new File(bufferwriteRestoreFilePath);
		if (restoreFile.exists()) {
			restoreFile.delete();
		}
	}

	/**
	 * The left of the pair is the last position. The right of the pair is the
	 * rowGroupMetadata.
	 *
	 * @return - the left is the end position of the last rowgroup flushed, the
	 *         right is all the rowgroup meatdata flushed
	 * @throws IOException
	 */
	private Pair<Long, List<RowGroupMetaData>> ReadStoreFromDisk() throws IOException {
		byte[] lastPostionBytes = new byte[TSFILEPOINTBYTESIZE];
		List<RowGroupMetaData> groupMetaDatas = new ArrayList<>();
		RandomAccessFile randomAccessFile = null;
		try {
			randomAccessFile = new RandomAccessFile(bufferwriteRestoreFilePath, "rw");
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
				lastRowgroupSize = groupMetaDatas.size();
				point = randomAccessFile.getFilePointer();
			}
			// read the tsfile position information using byte[8] which is
			// present one long number.
			randomAccessFile.read(lastPostionBytes);
		} catch (FileNotFoundException e) {
			LOGGER.error("The restore file is not exist, the restore file path is {}.", bufferwriteRestoreFilePath);
			throw e;
		} catch (IOException e) {
			LOGGER.error("Read data from file error.");
			throw e;
		} finally {
			if (randomAccessFile != null) {
				randomAccessFile.close();
			}
		}
		long lastPostion = BytesUtils.bytesToLong(lastPostionBytes);
		Pair<Long, List<RowGroupMetaData>> result = new Pair<Long, List<RowGroupMetaData>>(lastPostion, groupMetaDatas);
		return result;
	}

	private FileSchema constructFileSchema(String processorName) throws PathErrorException, WriteProcessException {
		List<ColumnSchema> columnSchemaList;

		columnSchemaList = mManager.getSchemaForFileName(processorName);
		FileSchema fileSchema = null;
		try {
			fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, processorName);
		} catch (WriteProcessException e) {
			LOGGER.error("Get the FileSchema error, the list of ColumnSchema is {}.", columnSchemaList);
			throw e;
		}
		return fileSchema;
	}

	private FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String processorName)
			throws WriteProcessException {
		JSONArray rowGroup = new JSONArray();
		for (ColumnSchema col : schemaList) {
			rowGroup.put(constrcutMeasurement(col));
		}
		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, processorName);
		return new FileSchema(jsonSchema);
	}

	private JSONObject constrcutMeasurement(ColumnSchema col) {
		JSONObject measurement = new JSONObject();
		measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
		measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
		measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
		for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
			if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
				String[] valueArray = entry.getValue().split(",");
				measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
			} else
				measurement.put(entry.getKey(), entry.getValue().toString());
		}
		return measurement;
	}

	public String getFileName() {
		return fileName;
	}

	public String getFileRelativePath() {
		return bufferwriterelativePath;
	}

	public boolean isNewProcessor() {
		return isNewProcessor;
	}

	public void setNewProcessor(boolean isNewProcessor) {
		this.isNewProcessor = isNewProcessor;
	}

	/**
	 * Write a data point
	 *
	 * @param deltaObjectId
	 * @param measurementId
	 * @param timestamp
	 * @param dataType
	 * @param value
	 * @throws BufferWriteProcessorException
	 * @throws IOException
	 */
	public void write(String deltaObjectId, String measurementId, long timestamp, TSDataType dataType, String value)
			throws BufferWriteProcessorException {
		TSRecord record = new TSRecord(timestamp, deltaObjectId);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, value);
		record.addTuple(dataPoint);
		write(record);
	}

	/**
	 * Write a tsRecord
	 *
	 * @param tsRecord
	 * @throws BufferWriteProcessorException
	 */
	public void write(TSRecord tsRecord) throws BufferWriteProcessorException {

		try {
			long newMemUsage = MemUtils.getTsRecordMemBufferwrite(tsRecord);
			BasicMemController.UsageLevel level = BasicMemController.getInstance().reportUse(this, newMemUsage);
			switch (level) {
			case SAFE:
				recordWriter.write(tsRecord);
				memUsed += newMemUsage;
				break;
			case WARNING:
				LOGGER.debug("Memory usage will exceed warning threshold, current : {}.",
						MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
				recordWriter.write(tsRecord);
				memUsed += newMemUsage;
				break;
			case DANGEROUS:
			default:
				LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}.",
						MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
				throw new BufferWriteProcessorException("Memory usage exceeded dangerous threshold.");
			}
		} catch (IOException | WriteProcessException e) {
			LOGGER.error("Write TSRecord error, the TSRecord is {}, the bufferwrite is {}.", tsRecord,
					getProcessorName());
			throw new BufferWriteProcessorException(e);
		}
	}

	public Pair<List<Object>, List<RowGroupMetaData>> getIndexAndRowGroupList(String deltaObjectId,
			String measurementId) {
		List<Object> memData = null;
		List<RowGroupMetaData> list = null;
		// wait until flush over
		synchronized (flushState) {
			while (flushState.isFlushing()) {
				try {
					flushState.wait();
				} catch (InterruptedException e) {
					LOGGER.error("Interrupted from waitting to flush.");
				}
			}
		}
		convertBufferLock.readLock().lock();
		try {
			memData = recordWriter.getDataInMemory(deltaObjectId, measurementId);
			list = bufferIOWriter.getCurrentRowGroupMetaList(deltaObjectId);
		} finally {
			convertBufferLock.readLock().unlock();
		}
		return new Pair<>(memData, list);
	}

	@Override
	public boolean canBeClosed() {
		LOGGER.info("Check bufferwrite {} can be closed or not.", getProcessorName());
		if (flushState.isFlushing()) {
			LOGGER.info("The bufferwrite {} can't be closed.", getProcessorName());
			return false;
		} else {
			LOGGER.info("The bufferwrite {} can be closed.", getProcessorName());
			return true;
		}
	}

	@Override
	public void flush() throws IOException {
		recordWriter.flushRowGroup(false);
	}

	@Override
	public void close() throws BufferWriteProcessorException {
		isFlushingSync = true;
		try {
			recordWriter.close();
			// update the intervalfile for interval list
			bufferwriteCloseAction.act();
			// flush the changed information for filenode
			filenodeFlushAction.act();
			// delete the restore for this bufferwrite processor
			deleteRestoreFile();
		} catch (IOException e) {
			LOGGER.error("Close the bufferwrite processor error, the bufferwrite is {}.", getProcessorName());
			throw new BufferWriteProcessorException(e);
		} catch (Exception e) {
			LOGGER.error("Close the bufferwrite processor failed, when call the action function.");
			throw new BufferWriteProcessorException(e);
		} finally {
			isFlushingSync = false;
		}
	}

	@Override
	public long memoryUsage() {
		return recordWriter.getMemoryUsage();
	}

	public void addTimeSeries(String measurementToString, String dataType, String encoding, String[] encodingArgs)
			throws IOException {
		ColumnSchema col = new ColumnSchema(measurementToString, TSDataType.valueOf(dataType),
				TSEncoding.valueOf(encoding));
		JSONObject measurement = constrcutMeasurement(col);
		try {
			recordWriter.addMeasurementByJson(measurement);
		} catch (WriteProcessException e) {
			throw new IOException(e);
		}
	}

	private class BufferWriteRecordWriter extends TsFileWriter {

		private Map<String, IRowGroupWriter> flushingRowGroupWriters;
		private Set<String> flushingRowGroupSet;
		private long flushingRecordCount;
		private long lastFlushTime = -1;

		BufferWriteRecordWriter(TSFileConfig conf, BufferWriteIOWriter ioFileWriter, FileSchema schema)
				throws WriteProcessException {
			super(ioFileWriter, schema, conf);
		}

		/**
		 * insert a list of data value in form of TimePair.
		 *
		 * @param record
		 *            - TSRecord to be written
		 * @throws Exception
		 * @throws WriteProcessException
		 * @throws IOException
		 */
		@Override
		public void write(TSRecord record) throws IOException, WriteProcessException {
			try {
				super.write(record);
			} catch (IOException | WriteProcessException e) {
				LOGGER.error("Write TSRecord error, TSRecord is {}.", record);
				throw e;
			}
		}

		@Override
		protected void flushRowGroup(boolean isFillRowGroup) throws IOException {
			// calculate the time interval between last flush and this flush
			if (lastFlushTime > 0) {
				long thisFlushTime = System.currentTimeMillis();
				long flushTimeInterval = thisFlushTime - lastFlushTime;
				DateTime lastDateTime = new DateTime(lastFlushTime,
						TsfileDBDescriptor.getInstance().getConfig().timeZone);
				DateTime thisDateTime = new DateTime(thisFlushTime,
						TsfileDBDescriptor.getInstance().getConfig().timeZone);
				LOGGER.info("Last flush time is {}, this flush time is {}, flush time interval is {}", lastDateTime,
						thisDateTime, flushTimeInterval);
				lastFlushTime = thisFlushTime;
			}
			if (recordCount > 0) {
				synchronized (flushState) {
					// This thread wait until the subThread flush finished
					while (flushState.isFlushing()) {
						try {
							flushState.wait();
						} catch (InterruptedException e) {
							LOGGER.error("Interrupt error when waitting to flush, the processor:{}.",
									getProcessorName(), e);
						}
					}
				}
				long oldMemUsage = memUsed;
				memUsed = 0;
				// update the lastUpdatetime
				try {
					bufferwriteFlushAction.act();
				} catch (Exception e) {
					LOGGER.error("Flush bufferwrite row group failed, when call the action function.");
					throw new IOException(e);
				}

				if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
					// For WAL
					logNode.notifyStartFlush();
				}

				// flush bufferwrite data
				if (isFlushingSync) {
					try {
						LOGGER.info("{} bufferwrite start to flush synchronously,-Thread id {}.", getProcessorName(),
								Thread.currentThread().getName());
						super.flushRowGroup(false);
						writeStoreToDisk();
						filenodeFlushAction.act();
						if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
							logNode.notifyEndFlush(null);
						}
						LOGGER.info("{} bufferwrite end to flush synchronously,-Thread id {}.", getProcessorName(),
								Thread.currentThread().getName());
					} catch (IOException e) {
						LOGGER.error("Flush row group to store failed, processor:{}.", getProcessorName());
						throw e;
					} catch (BufferWriteProcessorException e) {
						// write restore error
						LOGGER.error("Write bufferwrite information to disk failed.");
						throw new IOException(e);
					} catch (Exception e) {
						// action error
						LOGGER.error("Flush bufferwrite row group failed, when call the action function.");
						// handle
						throw new IOException(e);
					}
					BasicMemController.getInstance().reportFree(BufferWriteProcessor.this, oldMemUsage);
					checkSize();
				} else {
					flushState.setFlushing();
					switchIndexFromWorkToFlush();
					switchRecordWriterFromWorkToFlush();

					Runnable flushThread;
					flushThread = () -> {
						LOGGER.info("{} bufferwrite start to flush asynchronously,-Thread id {}.", getProcessorName(),
								Thread.currentThread().getName());
						try {
							asyncFlushRowGroupToStore();
							writeStoreToDisk();
							filenodeFlushAction.act();
							if (TsfileDBDescriptor.getInstance().getConfig().enableWal) {
								logNode.notifyEndFlush(null);
							}
						} catch (IOException e) {
							/*
							 * There should be added system log by CGF and throw
							 * exception
							 */
							LOGGER.error(String.format("%s asynchronous flush error, sleep this thread-%s.",
									getProcessorName(), Thread.currentThread().getName()), e);
							// TODO
						} catch (BufferWriteProcessorException e) {
							LOGGER.error("Write bufferwrite information to disk failed.", e);
							// how to handle this error
							// TODO
						} catch (Exception e) {
							// action error
							LOGGER.error("Flush bufferwrite row group failed, when call the action function.", e);
							// how to handle this error
							// TODO
						}
						switchRecordWriterFromFlushToWork();
						convertBufferLock.writeLock().lock();
						try {
							synchronized (flushState) {
								switchIndexFromFlushToWork();
								flushState.setUnFlushing();
								flushState.notify();
								LOGGER.info("{} bufferwrite end to flush asynchronously,-Thread id {}.",
										getProcessorName(), Thread.currentThread().getName());
							}
						} finally {
							convertBufferLock.writeLock().unlock();
						}
						BasicMemController.getInstance().reportFree(BufferWriteProcessor.this, oldMemUsage);
						checkSize();
					};
					FlushManager.getInstance().submit(flushThread);
				}
			}
		}

		private void asyncFlushRowGroupToStore() throws IOException {

			if (flushingRecordCount > 0) {
				long startFlushTime = System.currentTimeMillis();
				long totalMemStart = deltaFileWriter.getPos();
				for (String deltaObjectId : flushingRowGroupSet) {
					long rowGroupStart = deltaFileWriter.getPos();
					deltaFileWriter.startRowGroup(flushingRecordCount, deltaObjectId);
					IRowGroupWriter groupWriter = flushingRowGroupWriters.get(deltaObjectId);
					groupWriter.flushToFileWriter(deltaFileWriter);
					deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - rowGroupStart);
				}
				long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
				long timeInterval = System.currentTimeMillis() - startFlushTime;
				if (timeInterval == 0) {
					timeInterval = 1;
				}
				// remove the feature: fill the row group
				// fillInRowGroupSize(actualTotalRowGroupSize);
				LOGGER.info(
						"{} asynchronous flush total row group size:{}, actual:{}, less:{}, time consume:{} ms, flush rate:{} bytes/ms",
						getProcessorName(), primaryRowGroupSize, actualTotalRowGroupSize,
						primaryRowGroupSize - actualTotalRowGroupSize, timeInterval,
						actualTotalRowGroupSize / timeInterval);
			}
		}

		private void switchRecordWriterFromWorkToFlush() {

			flushingRowGroupWriters = groupWriters;
			flushingRowGroupSet = new HashSet<>();
			for (String DeltaObjectId : schema.getDeltaObjectAppearedSet()) {
				flushingRowGroupSet.add(DeltaObjectId);
			}
			flushingRecordCount = recordCount;
			// reset
			groupWriters = new HashMap<String, IRowGroupWriter>();
			schema.getDeltaObjectAppearedSet().clear();
			recordCount = 0;
		}

		private void switchRecordWriterFromFlushToWork() {
			flushingRowGroupSet = null;
			flushingRowGroupWriters = null;
			flushingRecordCount = -1;
		}
	}

	private void switchIndexFromWorkToFlush() {

	}

	private void switchIndexFromFlushToWork() {
		bufferIOWriter.addNewRowGroupMetaDataToBackUp();
	}

	/**
	 * @return The sum of all timeseries's metadata size within this file.
	 */
	public long getMetaSize() {
		// TODO : [MemControl] implement this
		return 0;
	}

	/**
	 * @return The file size of the TsFile corresponding to this processor.
	 */
	public long getFileSize() {
		// TODO : save this variable to avoid object creation?
		File file = new File(bufferwriteOutputFilePath);
		return file.length();
	}

	/**
	 * Close current TsFile and open a new one for future writes. Block new
	 * writes and wait until current writes finish.
	 */
	public void rollToNewFile() {
		// TODO : [MemControl] implement this
	}

	/**
	 * Check if this TsFile has too big metadata or file. If true, close current
	 * file and open a new one.
	 */
	private void checkSize() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		long metaSize = getMetaSize();
		long fileSize = getFileSize();
		if (metaSize >= config.bufferwriteMetaSizeThreshold || fileSize >= config.bufferwriteFileSizeThreshold) {
			LOGGER.info("{} size reaches threshold, closing. meta size is {}, file size is {}", this.fileName,
					MemUtils.bytesCntToStr(metaSize), MemUtils.bytesCntToStr(fileSize));
			rollToNewFile();
		}
	}
}
