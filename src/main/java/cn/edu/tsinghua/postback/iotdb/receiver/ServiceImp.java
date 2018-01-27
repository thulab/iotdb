package cn.edu.tsinghua.postback.iotdb.receiver;

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
	private String uuid;
	private Map<String, List<String>> newFilesMap = new HashMap<>(); // String means Storage Group, List means the set
																		// of
																		// new Files(AbsulutePath) in local IoTDB
	private Map<String, Map<String, Long>> newFilesStartTime = new HashMap<>(); // String means AbsulutePath of new
																				// Files, Map String1 means
																				// timeseries、 String2 means
																				// startTime
	private Map<String, Map<String, Long>> newFilesEndTime = new HashMap<>();// String means AbsulutePath of new
																				// Files, Map String1 means timeseries、
																				// String2 means startTime
	private Map<String, List<String>> oldFilesMap = new HashMap<>();

	private Map<String, String> linkFilePath = new HashMap<>();

	private Set<String> SQLToMerge = new HashSet<>(); // SQL for data of the seconde type
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceImp.class);
	private PostBackConfig config = PostBackDescriptor.getInstance().getConfig();
	private int fileNum = 0;
	private int fileNum_NewFiles = 0;
	private int fileNum_OldFiles = 0;
	private static final FileNodeManager fileNodeManager = FileNodeManager.getInstance();
	private static final MManager mManager = MManager.getInstance();

	public String getUUID(String uuid) throws TException {
		this.uuid = uuid;
		fileNum = 0;
		fileNum_NewFiles = 0;
		fileNum_OldFiles = 0;
		return this.uuid;
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
		filePath = config.IOTDB_DATA_DIRECTORY + uuid + File.separator + filePath;
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
				fos = new FileOutputStream(file, true);
				channel = fos.getChannel();
				channel.write(dataToReceive);
				channel.close();
				fos.close();
			} catch (Exception e) {
				LOGGER.error("IoTDB post back receicer: cannot write data to file because {}", e.getMessage());
			}
		} else { // the data has received successfully
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
					fileNum++;
					LOGGER.info("IoTDB post back receicer : Receiver has received " + fileNum + " files from sender!");
				}
			} catch (Exception e) {
				LOGGER.error("IoTDB post back receicer: cannot generate md5 because {}", e.getMessage());
			}
		}
		return md5OfReceiver;
	}

	public void getSchema(String sql) throws TException {
		Connection connection = null;
		Statement statement = null;
		try {
			Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
			connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
			statement = connection.createStatement();
			statement.execute(sql);
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

	/**
	 * Close connection of derby database after receiving all files from * sender
	 * side
	 */
	public void afterReceiving() throws TException {
		judgeMergeType();
		SQLToMerge.clear();
		if (fileNum_OldFiles != 0) {
			getSqlToMerge();
		}
		insertSQL();
		mergeNewData();
		deleteFile(new File(config.IOTDB_DATA_DIRECTORY + uuid));
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
		String filePath = config.IOTDB_DATA_DIRECTORY + uuid + File.separator + "delta";
		File root = new File(filePath);
		File[] files = root.listFiles();
		oldFilesMap.clear();
		newFilesMap.clear();
		int num = 0;
		linkFilePath.clear();
		for (File file : files) {
			String storageGroupPathPB = config.IOTDB_DATA_DIRECTORY + uuid + File.separator + "delta" + File.separator
					+ file.getName();
			File storageGroupPB = new File(storageGroupPathPB);
			File[] filesSG = storageGroupPB.listFiles();
		}
		for (File file : files) {
			String storageGroupPath = config.IOTDB_DATA_DIRECTORY + "delta" + File.separator + file.getName();
			String storageGroupPathPB = config.IOTDB_DATA_DIRECTORY + uuid + File.separator + "delta" + File.separator
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
					linkFilePath.put(fileTF.getAbsolutePath(), storageGroupPath + File.separator + fileTF.getName());
					newFilesStartTime.put(fileTF.getAbsolutePath(), startTimeMap);
					newFilesEndTime.put(fileTF.getAbsolutePath(), endTimeMap);
					newFiles.add(fileTF.getAbsolutePath());
					num++;
					LOGGER.info("IoTDB receiver : Judging MERGE_TYPE has complete : " + num + "/" + fileNum);
				}
				// .restore file will create when SET and CREATE and flush
				newFilesMap.put(file.getName(), newFiles);
				fileNum_NewFiles += newFiles.size();
			} else // the two other types:new tsFile but not new Storage Group , not new tsFile
			{
				Connection connection = null;
				Statement statement = null;
				try {
					Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
					connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
					statement = connection.createStatement();
					statement.execute("flush");
					fileNodeManager.mergeAll();
				} catch (SQLException | ClassNotFoundException| FileNodeManagerException e) {
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
				
				List<String> newFiles = new ArrayList<>();
				List<String> oldFiles = new ArrayList<>();
				newFiles.clear();
				oldFiles.clear();
				Map<String, Long> timeseriesEndTimeMap = new HashMap<>();
				timeseriesEndTimeMap.clear();
				File[] filesSG = storageGroup.listFiles();
				// get all timeseries detail endTime
				for (File fileTF : filesSG) {
					TsRandomAccessLocalFileReader input = null;
					try {
						input = new TsRandomAccessLocalFileReader(
								fileTF.getAbsolutePath());
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
								List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
										.getTimeSeriesChunkMetaDataList();
								for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
									TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
											.getTInTimeSeriesChunkMetaData();
									TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
									String measurementUID = properties.getMeasurementUID();
									long endTime = tInTimeSeriesChunkMetaData.getEndTime();
									measurementUID = key + "." + measurementUID;
									if (!timeseriesEndTimeMap.containsKey(measurementUID))
										timeseriesEndTimeMap.put(measurementUID, endTime);
									else {
										if (timeseriesEndTimeMap.get(measurementUID) < endTime)
											timeseriesEndTimeMap.put(measurementUID, endTime);
									}
								}
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
				}
				// FileNodeProcessor fileNodeProcessor = null;
				// try {
				// fileNodeProcessor = fileNodeManager.getProcessor(file.getName(), true);
				// timeseriesEndTimeMap = fileNodeProcessor.getLastUpdateTimeMap();
				// } catch (FileNodeManagerException e1) {
				// // TODO Auto-generated catch block
				// e1.printStackTrace();
				// } finally {
				// fileNodeProcessor.writeUnlock();
				// }
				//
				// judge uuid TsFile is new file or not
				filesSG = storageGroupPB.listFiles();
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
							String key = it.next().toString(); // key represent storage group
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
									if (timeseriesEndTimeMap.containsKey(measurementUID)
											&& timeseriesEndTimeMap
													.get(measurementUID) >= startTime) {
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
						linkFilePath.put(fileTF.getAbsolutePath(),
								storageGroupPath + File.separator + fileTF.getName());
						String newFilePath = fileTF.getAbsolutePath();
						newFilesStartTime.put(newFilePath, startTimeMap);
						newFilesEndTime.put(newFilePath, endTimeMap);
						newFiles.add(newFilePath);
					} else {
						oldFiles.add(fileTF.getAbsolutePath());
					}
					num++;
					LOGGER.info("IoTDB receiver : Judging MERGE_TYPE has complete : " + num + "/" + fileNum);
				}
				if (newFiles.size() != 0) {
					newFilesMap.put(file.getName(), newFiles);
					fileNum_NewFiles += newFiles.size();
				}
				if (oldFiles.size() != 0) {
					oldFilesMap.put(file.getName(), oldFiles);
					fileNum_OldFiles += oldFiles.size();
				}
			}
		}
	}

	public void getSqlToMerge() throws TException {
		Iterator<String> iterator = oldFilesMap.keySet().iterator();
		int num = 0;
		while (iterator.hasNext()) {
			List<String> oldFiles = oldFilesMap.get(iterator.next());
			for (String filePath : oldFiles) {
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
								SQLToMerge.add(sql);
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
				LOGGER.info("IoTDB receiver : Merging old files has completed : " + num + "/" + fileNum_OldFiles);
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
			for (String sql : SQLToMerge) {
				statement.execute(sql);
			}
			statement.execute("flush");
			fileNodeManager.mergeAll();
		} catch (SQLException | ClassNotFoundException | FileNodeManagerException e) {
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
		for (String storageGroup : newFilesMap.keySet()) {
			List<String> newFilePath = newFilesMap.get(storageGroup);
			// before load extern tsFile, it is necessary to order files in the same SG
			for (int i = 0; i < newFilePath.size(); i++) {
				for (int j = i + 1; j < newFilePath.size(); j++) {
					boolean swapOrNot = false;
					Map<String, Long> startTimeI = newFilesStartTime.get(newFilePath.get(i));
					Map<String, Long> endTimeI = newFilesStartTime.get(newFilePath.get(i));
					Map<String, Long> startTimeJ = newFilesStartTime.get(newFilePath.get(j));
					Map<String, Long> endTimeJ = newFilesStartTime.get(newFilePath.get(j));
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
				Map<String, Long> startTimeMap = newFilesStartTime.get(path);
				Map<String, Long> endTimeMap = newFilesEndTime.get(path);

				// create a new fileNode
				String header = config.IOTDB_DATA_DIRECTORY + uuid + File.separator + "delta" + File.separator;
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
				String linkPath = linkFilePath.get(path);
				java.nio.file.Path link = FileSystems.getDefault().getPath(linkPath);
				java.nio.file.Path target = FileSystems.getDefault().getPath(path);
				try {
					Files.createLink(link, target);
				} catch (IOException e) {
					LOGGER.error("IoTDB receiver : Cannot create a link for file : {}", path);
				}
				
				num++;
				LOGGER.info("IoTDB receiver : Merging new files has completed : " + num + "/" + fileNum_NewFiles);
			}
		}
	}

	public Set<String> getSQLToMerge() {
		return SQLToMerge;
	}

	public Map<String, List<String>> getOldFilesMap() {
		return oldFilesMap;
	}

	public void setOldFilesMap(Map<String, List<String>> oldFilesMap) {
		this.oldFilesMap = oldFilesMap;
	}
}