package cn.edu.tsinghua.postback.iotdb.receiver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessorStatus;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessorStore;
import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.filenode.OverflowChangeType;
import cn.edu.tsinghua.iotdb.engine.filenode.SerializeUtil;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

public class ServiceImp implements Service.Iface {
	private ThreadLocal<String> uuid = new ThreadLocal<String>();
	private ThreadLocal<Map<String, List<String>>> newFilesMap = new ThreadLocal<>(); // String means Storage Group, List means the set
																		// of
																		// new Files(AbsulutePath) in local IoTDB
	private ThreadLocal<Map<String, Map<String, Long>>> newFilesStartTime = new ThreadLocal<>(); // String means AbsulutePath of new
																				// Files, Map String1 means
																				// timeseries、 String2 means
																				// startTime
	private ThreadLocal<Map<String, Map<String, Long>>> newFilesEndTime = new ThreadLocal<>();// String means AbsulutePath of new
																				// Files, Map String1 means timeseries、
																				// String2 means startTime
	private ThreadLocal<Map<String, List<String>>> oldFilesMap = new ThreadLocal<>();

	private ThreadLocal<Map<String, String>> linkFilePath = new ThreadLocal<>();

	private ThreadLocal<Set<String>> SQLToMerge = new ThreadLocal<>(); // SQL for data of the seconde type
	private PostBackConfig config = PostBackDescriptor.getInstance().getConfig();
	private ThreadLocal<Integer> fileNum = new ThreadLocal<Integer>();
	private ThreadLocal<Integer> fileNum_NewFiles = new ThreadLocal<Integer>();
	private ThreadLocal<Integer> fileNum_OldFiles = new ThreadLocal<Integer>();
	private ThreadLocal<String> schemaFromSenderPath = new ThreadLocal<String>();
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceImp.class);
	private static final FileNodeManager fileNodeManager = FileNodeManager.getInstance();
	private static final MManager mManager = MManager.getInstance();

	public void init() {
		fileNum.set(0); 
		fileNum_NewFiles.set(0); 
		fileNum_OldFiles.set(0);
		newFilesMap.set(new HashMap<>());
		newFilesStartTime.set(new HashMap<>());
		newFilesEndTime.set(new HashMap<>());
		oldFilesMap.set(new HashMap<>());
		linkFilePath.set(new HashMap<>());
		SQLToMerge.set(new HashSet<>());
		schemaFromSenderPath.set(config.IOTDB_DATA_DIRECTORY + uuid.get() + File.separator + "mlog.txt");
	}
	
	public String getUUID(String uuid) throws TException {
		this.uuid.set(uuid);
		init();
		if(new File(config.IOTDB_DATA_DIRECTORY + this.uuid.get()).exists() && new File(config.IOTDB_DATA_DIRECTORY + this.uuid.get()).list().length!=0) {
			// if does not exist, it means that the last time postback failed, clear uuid data and receive the data again
			deleteFile(new File(config.IOTDB_DATA_DIRECTORY + this.uuid.get()));
		}
		return this.uuid.get();
	}

	public String startReceiving(String md5, List<String> filePathSplit, ByteBuffer dataToReceive, int status)
			throws TException {
		String md5OfReceiver = "";
		String filePath = "";
		FileOutputStream fos = null;
		FileChannel channel = null;
		for (int i = 0; i < filePathSplit.size(); i++) {
			if (i == filePathSplit.size() - 1) {
				filePath = filePath + filePathSplit.get(i);
			} else {
				filePath = filePath + filePathSplit.get(i) + File.separator;
			}
		}
		filePath = config.IOTDB_DATA_DIRECTORY + uuid.get() + File.separator + filePath;
		if (status == 1) // there are still data stream to add
		{
			File file = new File(filePath);
			if (!file.getParentFile().exists()) {
				try {
					file.getParentFile().mkdirs();
					file.createNewFile();
				} catch (IOException e) {
					LOGGER.error("IoTDB post back receicer: cannot make file because {}", e.getMessage());
				}
			}
			try {
				fos = new FileOutputStream(file, true); //append new data
				channel = fos.getChannel();
				channel.write(dataToReceive);
				channel.close();
				fos.close();
			} catch (Exception e) {
				LOGGER.error("IoTDB post back receicer: cannot write data to file because {}", e.getMessage());
			}
		} else {                                        // all data in the same file has received successfully
			try {
				FileInputStream fis = new FileInputStream(filePath);
				MessageDigest md = MessageDigest.getInstance("MD5");
				int mBufferSize = 4 * 1024 * 1024;
				byte[] buffer = new byte[mBufferSize];
				int n;
				while ((n = fis.read(buffer)) != -1) {
					md.update(buffer, 0, n);
				}
				fis.close();
				md5OfReceiver = (new BigInteger(1, md.digest())).toString(16);
				fis.close();
				if (md5.equals(md5OfReceiver)) {
					fileNum.set(fileNum.get()+1);
					LOGGER.info("IoTDB post back receicer : Receiver has received " + fileNum.get() + " files from sender!");
				}
			} catch (Exception e) {
				LOGGER.error("IoTDB post back receicer: cannot generate md5 because {}", e.getMessage());
			}
		}
		return md5OfReceiver;
	}

	public void getSchema(ByteBuffer schema, int status) throws TException {
		FileOutputStream fos = null;
		FileChannel channel = null;
		if(status == 0) {
			Connection connection = null;
			Statement statement = null;
			try {
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
				statement = connection.createStatement();
				
				BufferedReader bf;
				try {
					bf = new BufferedReader(new java.io.FileReader(schemaFromSenderPath.get()));
					String data;
					statement.clearBatch();
					while ((data = bf.readLine()) != null) {
						String item[] = data.split(",");
						if (item[0].equals("2")) {
							String sql = "SET STORAGE GROUP TO " + item[1];
							statement.addBatch(sql);
						} else if (item[0].equals("0")) {
							String sql = "CREATE TIMESERIES " + item[1] + " WITH DATATYPE=" + item[2] + ", ENCODING=" + item[3];
							statement.addBatch(sql);
						}
					}
					bf.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				statement.executeBatch();
			} catch (SQLException | ClassNotFoundException e) {
				LOGGER.error("IoTDB post back receicer: jdbc cannot connect to IoTDB because {}", e.getMessage());
			} finally {
				try {
					if(statement!=null)
						statement.close();
					if(connection!=null)
						connection.close();
				} catch (SQLException e) {
					LOGGER.error("IoTDB receiver : can not close JDBC connection because {}", e.getMessage());
				}
			}
		}
		else {
			File file = new File(schemaFromSenderPath.get());
			if (!file.getParentFile().exists()) {
				try {
					file.getParentFile().mkdirs();
					file.createNewFile();
				} catch (IOException e) {
					LOGGER.error("IoTDB post back receicer: cannot make schema file because {}", e.getMessage());
				}
			}
			try {
				fos = new FileOutputStream(file, true);
				channel = fos.getChannel();
				channel.write(schema);
				channel.close();
				fos.close();
			} catch (Exception e) {
				LOGGER.error("IoTDB post back receicer: cannot write data to file because {}", e.getMessage());
			}
		}
	}

	/**
	 * Close connection of derby database after receiving all files from * sender
	 * side
	 */
	public void afterReceiving() throws TException {
		judgeMergeType();
		getSqlToMerge();
		mergeNewData();
		deleteFile(new File(config.IOTDB_DATA_DIRECTORY + uuid.get()));
		remove();
	}
	
	public void remove() {
		uuid.remove();
		fileNum.remove();
		fileNum_NewFiles.remove(); 
		fileNum_OldFiles.remove();
		newFilesMap.remove();
		newFilesStartTime.remove();
		newFilesEndTime.remove();
		oldFilesMap.remove();
		linkFilePath.remove();
		schemaFromSenderPath.remove();
		SQLToMerge.remove();
	}

	private void deleteFile(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} else {
			File[] files = file.listFiles();
			for (File f : files) {
				deleteFile(f);
				f.delete();
			}
		}
	}

	/**
	 * Stop receiving files from sender side
	 */
	public void cancelReceiving() throws TException {

	}

	public void judgeMergeType() throws TException {
		String filePath = config.IOTDB_DATA_DIRECTORY + uuid.get() + File.separator + "delta";
		File root = new File(filePath);
		File[] files = root.listFiles();
		int num = 0;
		for (File file : files) {
			String storageGroupPathPB = config.IOTDB_DATA_DIRECTORY + uuid.get() + File.separator + "delta" + File.separator
					+ file.getName();
			File storageGroupPB = new File(storageGroupPathPB);
			File[] filesSG = storageGroupPB.listFiles();
		}
		for (File file : files) {
			String storageGroupPath = config.IOTDB_DATA_DIRECTORY + "delta" + File.separator + file.getName();
			String storageGroupPathPB = config.IOTDB_DATA_DIRECTORY + uuid.get() + File.separator + "delta" + File.separator
					+ file.getName();
			String digestPath = config.IOTDB_DATA_DIRECTORY + "digest" + File.separator + file.getName();
			File storageGroup = new File(storageGroupPath);
			File storageGroupPB = new File(storageGroupPathPB);
			File digest = new File(digestPath);
			if (!storageGroup.exists()) // the first type: new storage group
			{
				List<String> newFiles = new ArrayList<>();
				newFiles.clear();
				storageGroup.mkdirs();
				// copy the storage group
				File[] filesSG = storageGroupPB.listFiles();
				for (File fileTF : filesSG) { // file means TsFiles
					Map<String, Long> startTimeMap = new HashMap<>();
					Map<String, Long> endTimeMap = new HashMap<>();
					TsRandomAccessLocalFileReader input = null;
					try {
						input = new TsRandomAccessLocalFileReader(fileTF.getAbsolutePath());
						FileReader reader = new FileReader(input);
						Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
						Iterator<String> it = deltaObjectMap.keySet().iterator();
						while (it.hasNext()) {
							String key = it.next().toString(); // key represent storage group
							TsDeltaObject deltaObj = deltaObjectMap.get(key);
							TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
							blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
									deltaObj.offset, deltaObj.metadataBlockSize));
							List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
							for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
								long startTime = 0x7fffffffffffffffL;
								long endTime = 0;
								List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
										.getTimeSeriesChunkMetaDataList();
								for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
									TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
											.getTInTimeSeriesChunkMetaData();
									TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
									String measurementUID = properties.getMeasurementUID();
									measurementUID = key + "." + measurementUID;
									startTime = Math.min(tInTimeSeriesChunkMetaData.getStartTime(), startTime);
									endTime = Math.max(tInTimeSeriesChunkMetaData.getEndTime(), endTime);
								}
								startTimeMap.put(rowGroupMetaData.getDeltaObjectID(), startTime);
								endTimeMap.put(rowGroupMetaData.getDeltaObjectID(), endTime);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {
							input.close();
						} catch (IOException e) {
							LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}",
									fileTF.getAbsolutePath(), e.getMessage());
						}
					}
					linkFilePath.get().put(fileTF.getAbsolutePath(), storageGroupPath + File.separator + fileTF.getName());
					newFilesStartTime.get().put(fileTF.getAbsolutePath(), startTimeMap);
					newFilesEndTime.get().put(fileTF.getAbsolutePath(), endTimeMap);
					newFiles.add(fileTF.getAbsolutePath());
					num++;
					LOGGER.info("IoTDB receiver : Judging MERGE_TYPE has complete : " + num + "/" + fileNum.get());
				}
				// .restore file will create when SET and CREATE and flush
				newFilesMap.get().put(file.getName(), newFiles);
				fileNum_NewFiles.set(fileNum_NewFiles.get() + newFiles.size());
			} else // the two other types:new tsFile but not new Storage Group , not new tsFile
			{				
				List<String> newFiles = new ArrayList<>();
				List<String> oldFiles = new ArrayList<>();
				newFiles.clear();
				oldFiles.clear();
				Map<String, Long> timeseriesEndTimeMap = new HashMap<>();
				timeseriesEndTimeMap.clear();
				// get all timeseries detail endTime
				 FileNodeProcessor fileNodeProcessor = null;
				 try {
					 fileNodeProcessor = fileNodeManager.getProcessor(file.getName(), true);
					 timeseriesEndTimeMap = fileNodeProcessor.getLastUpdateTimeMap();
				 } catch (FileNodeManagerException e) {
						LOGGER.info("IoTDB receiver : can not get lastupdateTimeMap because {}", e.getMessage());
				 } finally {
					 fileNodeProcessor.writeUnlock();
				 }
				
				 //judge uuid TsFile is new file or not
				File[] filesSG = storageGroupPB.listFiles();
				for (File fileTF : filesSG) {
					Map<String, Long> startTimeMap = new HashMap<>();
					Map<String, Long> endTimeMap = new HashMap<>();
					endTimeMap.clear();
					startTimeMap.clear();
					boolean isNew = true;
					TsRandomAccessLocalFileReader input = null;
					try {
						input = new TsRandomAccessLocalFileReader(fileTF.getAbsolutePath());
						FileReader reader = new FileReader(input);
						Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
						Iterator<String> it = deltaObjectMap.keySet().iterator();
						while (it.hasNext()) {
							String key = it.next().toString(); // key represent device
							TsDeltaObject deltaObj = deltaObjectMap.get(key);
							TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
							blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
									deltaObj.offset, deltaObj.metadataBlockSize));
							List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
							for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
								long deltaObjectStartTime = 0x7fffffffffffffffL;
								long deltaObjectEndTime = 0;
								List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
										.getTimeSeriesChunkMetaDataList();
								for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
									TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
											.getTInTimeSeriesChunkMetaData();
									TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
									String measurementUID = properties.getMeasurementUID();
									measurementUID = key + "." + measurementUID;
									long startTime = tInTimeSeriesChunkMetaData.getStartTime();
									long endTime = tInTimeSeriesChunkMetaData.getEndTime();
									deltaObjectStartTime = Math.min(startTime, deltaObjectStartTime);
									deltaObjectEndTime = Math.max(endTime, deltaObjectEndTime);
									if (timeseriesEndTimeMap.containsKey(key)
											&& timeseriesEndTimeMap
													.get(key) >= startTime) {
										isNew = false;
									}
								}
								startTimeMap.put(rowGroupMetaData.getDeltaObjectID(), deltaObjectStartTime);
								endTimeMap.put(rowGroupMetaData.getDeltaObjectID(), deltaObjectEndTime);
								if (!isNew)
									break;
							}
							if (!isNew)
								break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {
							input.close();
						} catch (IOException e) {
							LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}",
									fileTF.getAbsolutePath(), e.getMessage());
						}
					}
					if (isNew) // if the file is new data and not new storage group , copy it
					{
						linkFilePath.get().put(fileTF.getAbsolutePath(),
								storageGroupPath + File.separator + fileTF.getName());
						String newFilePath = fileTF.getAbsolutePath();
						newFilesStartTime.get().put(newFilePath, startTimeMap);
						newFilesEndTime.get().put(newFilePath, endTimeMap);
						newFiles.add(newFilePath);
					} else {
						oldFiles.add(fileTF.getAbsolutePath());
					}
					num++;
					LOGGER.info("IoTDB receiver : Judging MERGE_TYPE has complete : " + num + "/" + fileNum.get());
				}
				if (newFiles.size() != 0) {
					newFilesMap.get().put(file.getName(), newFiles);
					fileNum_NewFiles.set(fileNum_NewFiles.get() + newFiles.size());
				}
				if (oldFiles.size() != 0) {
					oldFilesMap.get().put(file.getName(), oldFiles);
					fileNum_OldFiles.set(fileNum_OldFiles.get() + oldFiles.size());
				}
			}
		}
	}

	public void getSqlToMerge() throws TException {
		Iterator<String> iterator = oldFilesMap.get().keySet().iterator();
		int num = 0;
		while (iterator.hasNext()) {
			List<String> oldFiles = oldFilesMap.get().get(iterator.next());
			for (String filePath : oldFiles) {
				SQLToMerge.get().clear();
				Set<String> timeseries = new HashSet<>();
				TsRandomAccessLocalFileReader input = null;				
				try {
					input = new TsRandomAccessLocalFileReader(filePath);
					FileReader reader = new FileReader(input);
					Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
					Iterator<String> it = deltaObjectMap.keySet().iterator();
					while (it.hasNext()) {
						String key = it.next().toString(); // key represent storage group
						TsDeltaObject deltaObj = deltaObjectMap.get(key);
						TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
						blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
								deltaObj.offset, deltaObj.metadataBlockSize));
						List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
						for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
							// firstly, get all timeseries in the same RowGroupMetaData
							timeseries.clear();
							List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
									.getTimeSeriesChunkMetaDataList();
							for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
								TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
										.getTInTimeSeriesChunkMetaData();
								TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
								String measurementUID = properties.getMeasurementUID();
								long endTime = tInTimeSeriesChunkMetaData.getEndTime();
								measurementUID = key + "." + measurementUID;
								timeseries.add(measurementUID);
							}
							// secondly, use tsFile Reader to form SQL

							TsFile readTsFile;
							readTsFile = new TsFile(input);
							ArrayList<Path> paths = new ArrayList<>();
							paths.clear();
							for (String timesery : timeseries) {
								paths.add(new Path(timesery));
							}
							QueryDataSet queryDataSet = readTsFile.query(paths, null, null);
							while (queryDataSet.hasNextRecord()) {
								RowRecord record = queryDataSet.getNextRecord();
								List<Field> fields = record.getFields();
								String sql_front = null;
								for (Field field : fields) {
									if (field.toString() != "null") {
										sql_front = "insert into " + field.deltaObjectId + "(timestamp";
										break;
									}
								}
								String sql_rear = ") values(" + record.timestamp;
								for (Field field : fields) {
									if (field.toString() != "null") {
										sql_front = sql_front + "," + field.measurementId.toString();
										if (field.dataType == TSDataType.TEXT) {
											sql_rear = sql_rear + "," + "'" + field.toString() + "'";
										} else {
											sql_rear = sql_rear + "," + field.toString();
										}
									}
								}
								String sql = sql_front + sql_rear + ")";
								SQLToMerge.get().add(sql);
							}
						}
					}
				} catch (IOException e) {
					LOGGER.error("IoTDB receiver can not parse tsfile into SQL because{}", e.getMessage());
				} finally {
					try {
						input.close();
					} catch (IOException e) {
						LOGGER.error("IoTDB receiver : Cannot close file stream {} because {}",
								filePath);
					}
				}
				num++;
				LOGGER.info("IoTDB receiver : Merging old files has completed : " + num + "/" + fileNum_OldFiles.get());
				insertSQL();
			}
		}
		Connection connection = null;
		Statement statement = null;
		try {
			Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
			connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
			statement = connection.createStatement();
			statement.execute("flush");
		} catch (SQLException | ClassNotFoundException e) {
			LOGGER.error("IoTDB post back receicer: jdbc cannot connect to IoTDB because {}", e.getMessage());
		} finally {
			try {
				if(statement!=null)
					statement.close();
				if(connection!=null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("IoTDB receiver : can not close JDBC connection because {}", e.getMessage());
			}
		}
	}

	public void insertSQL() {
		Connection connection = null;
		Statement statement = null;
		try {
			Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
			connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
			statement = connection.createStatement();
			for (String sql : SQLToMerge.get()) {
				statement.addBatch(sql);
			}
			statement.executeBatch();
		} catch (SQLException | ClassNotFoundException e) {
			LOGGER.error("IoTDB post back receicer: jdbc cannot connect to IoTDB because {}", e.getMessage());
		} finally {
			try {
				if(statement!=null)
					statement.close();
				if(connection!=null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("IoTDB receiver : can not close JDBC connection because {}", e.getMessage());
			}
		}
	}

	public void mergeNewData() throws TException {
		// !!! Attention: before modify .restore file, it is neccessary to execute flush
		// order and synchronized the thread
		int num = 0;
		for (String storageGroup : newFilesMap.get().keySet()) {
			List<String> newFilePath = newFilesMap.get().get(storageGroup);
			// before load extern tsFile, it is necessary to order files in the same SG
			for (int i = 0; i < newFilePath.size(); i++) {
				for (int j = i + 1; j < newFilePath.size(); j++) {
					boolean swapOrNot = false;
					Map<String, Long> startTimeI = newFilesStartTime.get().get(newFilePath.get(i));
					Map<String, Long> endTimeI = newFilesStartTime.get().get(newFilePath.get(i));
					Map<String, Long> startTimeJ = newFilesStartTime.get().get(newFilePath.get(j));
					Map<String, Long> endTimeJ = newFilesStartTime.get().get(newFilePath.get(j));
					for (String deltaObject : endTimeI.keySet()) {
						if (startTimeJ.containsKey(deltaObject)
								&& startTimeI.get(deltaObject) > endTimeJ.get(deltaObject)) {
							swapOrNot = true;
							break;
						}
					}
					if (swapOrNot) {
						String temp = newFilePath.get(i);
						newFilePath.set(i, newFilePath.get(j));
						newFilePath.set(j, temp);
					}
				}
			}

			for (String path : newFilePath) {
				String fileNodeRestoreFilePath = config.IOTDB_DATA_DIRECTORY + "digest" + File.separator + storageGroup
						+ File.separator + storageGroup + ".restore";
				// get startTimeMap and endTimeMap
				Map<String, Long> startTimeMap = newFilesStartTime.get().get(path);
				Map<String, Long> endTimeMap = newFilesEndTime.get().get(path);

				// create a new fileNode
				String header = config.IOTDB_DATA_DIRECTORY + uuid.get() + File.separator + "delta" + File.separator;
				String relativePath = path.substring(header.length());
				IntervalFileNode fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,
						relativePath);

				// call inetrface of load external file
				try {
					fileNodeManager.appendFileToFileNode(storageGroup, fileNode);
				} catch (FileNodeManagerException e) {
					LOGGER.error("IoTDB receiver : can not load external file because {}", e.getMessage());
					;
				}

				// create link for new files , merge will erase all others which are not in the
				// fileNodemanager!
				String linkPath = linkFilePath.get().get(path);
				java.nio.file.Path link = FileSystems.getDefault().getPath(linkPath);
				java.nio.file.Path target = FileSystems.getDefault().getPath(path);
				try {
					Files.createLink(link, target);
				} catch (IOException e) {
					LOGGER.error("IoTDB receiver : Cannot create a link for file : {} , because {}", path, e.getMessage());
				}
				
				num++;
				LOGGER.info("IoTDB receiver : Merging new files has completed : " + num + "/" + fileNum_NewFiles.get());
			}
		}
	}

	public Set<String> getSQLToMerge() {
		return SQLToMerge.get();
	}

	public Map<String, List<String>> getOldFilesMap() {
		return oldFilesMap.get();
	}

	public void setOldFilesMap(Map<String, List<String>> oldFilesMap) {
		this.oldFilesMap.set(oldFilesMap);
	}
}