package cn.edu.tsinghua.iotdb.postback.sender;
/**
 * @author lta
 * The class is to transfer data that needs to postback to receiver. 
 */

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;
import cn.edu.tsinghua.iotdb.postback.receiver.ServerManager;
import cn.edu.tsinghua.iotdb.postback.receiver.Service;

public class TransferData {

	private TTransport transport;
	private Service.Client clientOfServer;
	private List<String> schema = new ArrayList<>();
	private String uuid;// Mark the identity of sender
	private boolean connectionOrElse; // Mark whether connection of sender and receiver has broken down or not
	private PostBackSenderConfig config = PostBackSenderDescriptor.getInstance().getConfig();
	private Date lastPostBackTime = new Date(); // Mark the start time of last postback
	private boolean postBackStatus = false; // If true, postback is in execution.
	private boolean clearOrNot = config.IS_CLEAR_ENABLE; // Clear data after postback has finished or not

	private static final Logger LOGGER = LoggerFactory.getLogger(TransferData.class);

	private static class TransferHolder {
		private static final TransferData INSTANCE = new TransferData();
	}

	private TransferData() {
	}

	public static final TransferData getInstance() {
		return TransferHolder.INSTANCE;
	}

	public void setConfig(PostBackSenderConfig config) {
		this.config = config;
	}

	private void getConnection(String serverIP, int serverPort) {
		connection(serverIP, serverPort);
		if (!connectionOrElse) {
			return;
		} else {
			if (!transferUUID(config.UUID_PATH)) {
				LOGGER.error(
						"IoTDB post back sender: Sorry! You do not have permission to connect to postback receiver!");
				connectionOrElse = false;
				return;
			}
		}
		return;
	}

	/**
	 * Eatablish a connection between sender and receiver
	 * 
	 * @param serverIP
	 * @param serverPort:it
	 *            must be same with port receiver set.
	 */
	private void connection(String serverIP, int serverPort) {
		transport = new TSocket(serverIP, serverPort);
		TProtocol protocol = new TBinaryProtocol(transport);
		clientOfServer = new Service.Client(protocol);
		try {
			transport.open();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back sender: IoTDB post back sender: cannot connect to server because {}",
					e.getMessage());
			connectionOrElse = false;
		}
	}

	/**
	 * UUID marks the identity of sender for receiver.
	 */
	private boolean transferUUID(String uuidPath) {
		File file = new File(uuidPath);
		BufferedReader bf;
		FileOutputStream out;
		if (!file.exists()) {
			try {
				file.createNewFile();
				uuid = "PB" + UUID.randomUUID().toString().replaceAll("-", "");
				out = new FileOutputStream(file);
				out.write(uuid.getBytes());
				out.close();
			} catch (Exception e) {
				LOGGER.error("IoTDB post back sender: cannot write UUID to file because {}", e.getMessage());
				connectionOrElse = false;
			}
		} else {
			try {
				bf = new BufferedReader(new FileReader(uuidPath));
				uuid = bf.readLine();
				bf.close();
			} catch (IOException e) {
				LOGGER.error("IoTDB post back sender: cannot read UUID from file because {}", e.getMessage());
				connectionOrElse = false;
			}
		}
		boolean leagalConnectionOrNot = true;
		try {
			leagalConnectionOrNot = clientOfServer.getUUID(uuid, InetAddress.getLocalHost().getHostAddress());
		} catch (TException e) {
			LOGGER.error("IoTDB sender: cannot send UUID to receiver because {}", e.getMessage());
			connectionOrElse = false;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			leagalConnectionOrNot = false;
		}
		return leagalConnectionOrNot;
	}

	/**
	 * Create snapshots for those sending files.
	 */
	private void makeFileSnapshot(Set<String> sendingFileList, String snapshotPath, String iotdbPath) {
		try {
			if (!new File(snapshotPath).exists())
				new File(snapshotPath).mkdir();
			for (String filePath : sendingFileList) {
				String relativeFilePath = filePath.substring(iotdbPath.length());
				String newPath = snapshotPath + File.separator + relativeFilePath;
				File newfile = new File(newPath);
				if (!newfile.getParentFile().exists()) {
					newfile.getParentFile().mkdirs();
				}
				Path link = FileSystems.getDefault().getPath(newPath);
				Path target = FileSystems.getDefault().getPath(filePath);
				Files.createLink(link, target);
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB sender: can not make fileSnapshot because {}", e.getMessage());
		}
	}

	/**
	 * Transfer data of a storage group to receiver.
	 * @param fileList : list of sending files in a storage group.
	 * @param snapshotPath
	 * @param iotdbPath
	 */
	private void startSending(Set<String> fileList, String snapshotPath, String iotdbPath) {
		try {
			int num = 0;
			for (String filePath : fileList) {
				num++;
				filePath = filePath.substring(iotdbPath.length());
				String dataPath = snapshotPath + File.separator + filePath;
				File file = new File(dataPath);
				List<String> filePathSplit = new ArrayList<>();
				String os = System.getProperty("os.name");
				if (os.toLowerCase().startsWith("windows")) {
					String[] name = filePath.split(File.separator + File.separator);
					for (String temp : name)
						filePathSplit.add(temp);
				} else {
					String[] name = filePath.split(File.separator);
					for (String temp : name)
						filePathSplit.add(temp);
				}
				while (true) {
					FileInputStream fis = new FileInputStream(file);
					MessageDigest md = MessageDigest.getInstance("MD5");
					int mBufferSize = 4 * 1024 * 1024;
					ByteArrayOutputStream bos = new ByteArrayOutputStream(mBufferSize);
					byte[] buffer = new byte[mBufferSize];
					int n;
					while ((n = fis.read(buffer)) != -1) { // cut the file into pieces to send
						bos.write(buffer, 0, n);
						ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
						bos.reset();
						md.update(buffer, 0, n);
						clientOfServer.startReceiving(null, filePathSplit, buffToSend, 1);
					}
					bos.close();
					fis.close();

					// the file is sent successfully
					String md5OfSender = (new BigInteger(1, md.digest())).toString(16);
					String md5OfReceiver = clientOfServer.startReceiving(md5OfSender, filePathSplit, null, 0);
					if (md5OfSender.equals(md5OfReceiver)) {
						LOGGER.info("IoTDB sender: receiver has received {} {}", filePath, "successfully.");
						break;
					}
				}
				LOGGER.info("IoTDB sender : Task of sending files to receiver has completed " + num + "/"
						+ fileList.size() + ".");
			}
		} catch (TException e) {
			LOGGER.error("IoTDB sender: cannot sending data because receiver has broken down.");
			connectionOrElse = false;
			return;
		} catch (Exception e) {
			LOGGER.error("IoTDB sender: cannot sending data because {}", e.getMessage());
			connectionOrElse = false;
			return;
		}
	}
	
	/**
	 * Sending schema to receiver.
	 * @param schemaPath
	 */
	private void sendSchema(String schemaPath) {
		try {
			FileInputStream fis = new FileInputStream(new File(schemaPath));
			int mBufferSize = 4 * 1024 * 1024;
			ByteArrayOutputStream bos = new ByteArrayOutputStream(mBufferSize);
			byte[] buffer = new byte[mBufferSize];
			int n;
			while ((n = fis.read(buffer)) != -1) { // cut the file into pieces to send
				bos.write(buffer, 0, n);
				ByteBuffer buffToSend = ByteBuffer.wrap(bos.toByteArray());
				bos.reset();
				clientOfServer.getSchema(buffToSend, 1); //1 represents there are still schema buffer to send
			}
			bos.close();
			fis.close();
			clientOfServer.getSchema(null, 0); //0 represents schema file has transfered completely.
		} catch (Exception e) {
			LOGGER.error("IoTDB sender : cannot send schema from mlog.txt because {}", e.getMessage());
			connectionOrElse = false;
		}
	}

	private boolean afterSending() {
		boolean successOrNot = false;
		try {
			successOrNot = clientOfServer.merge();
		} catch (TException e) {
			LOGGER.error("IoTDB sender : can not finish postback because postback receiver has broken down.");
			transport.close();
		}
		return successOrNot;
	}

	private void deleteSnapshot(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} else {
			File[] files = file.listFiles();
			for (File f : files) {
				deleteSnapshot(f);
				f.delete();
			}
		}
	}

	/**
	 * Delete date of a storage group after postback has finished.
	 */
	private void deleteData(Set<String> fileList, String snapshotPath, String iotdbPath) {

		Connection connection = null;
		Statement statement = null;
		TsRandomAccessLocalFileReader input = null;
		String deleteFormat = "delete from %s.* where time <= %s";
		try {
			Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
			connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
			statement = connection.createStatement();
			int count = 0;

			for (String filePath : fileList) {
				filePath = filePath.substring(iotdbPath.length());
				String dataPath = snapshotPath + File.separator + filePath;
				List<String> deleteSQL = new ArrayList<>();
				input = new TsRandomAccessLocalFileReader(dataPath);
				cn.edu.tsinghua.tsfile.timeseries.read.FileReader reader = new cn.edu.tsinghua.tsfile.timeseries.read.FileReader(
						input);
				Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
				Iterator<String> it = deltaObjectMap.keySet().iterator();
				while (it.hasNext()) {
					String key = it.next().toString(); // key represent device
					TsDeltaObject deltaObj = deltaObjectMap.get(key);
					String sql = String.format(deleteFormat, key, deltaObj.endTime);
					statement.addBatch(sql);
					count++;
					if (count > 100) {
						statement.executeBatch();
						statement.clearBatch();
						count = 0;
					}
				}
			}
			statement.executeBatch();
			statement.clearBatch();
		} catch (IOException e) {
			LOGGER.error("IoTDB receiver can not parse tsfile into delete SQL because{}", e.getMessage());
		} catch (SQLException | ClassNotFoundException e) {
			LOGGER.error("IoTDB post back receicer: jdbc cannot connect to IoTDB because {}", e.getMessage());
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				LOGGER.error("IoTDB receiver : Cannot close file stream because {}", e.getMessage());
			}
			try {
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				LOGGER.error("IoTDB receiver : Can not close JDBC connection because {}", e.getMessage());
			}
		}
	}

	public List<String> getSchema() {
		return schema;
	}

	public String getUuid() {
		return uuid;
	}

	/**
	 * Verify sending list is empty or not
	 * 
	 * @param sendingFileList
	 * @return
	 */
	private boolean isEmpty(Map<String, Set<String>> sendingFileList) {
		for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
			if (entry.getValue().size() != 0)
				return false;
		}
		return true;
	}

	public static void main(String[] args) {
		TransferData transferData = new TransferData();
		transferData.verifyPort();
		Thread monitor = new Thread(new Runnable() {
			public void run() {
				transferData.monitorPostbackStatus();
			}
		});
		monitor.start();
		transferData.timedTask();
	}

	/**
	 * The method is to verify whether the client port is bind or not, ensure that only one client is run.
	 */
	private void verifyPort() {
		try {  
            Socket socket = new Socket("localhost",config.CLIENT_PORT); 
            socket.close();
			LOGGER.error("The postback client has been started!");
			System.exit(0); 
        } catch (IOException e) {  
              try {
      			ServerSocket listenerSocket = new ServerSocket(config.CLIENT_PORT);
      			Thread listener = new Thread(new Runnable() {
      				public void run() {
      					while(true) {
      						try {
      							listenerSocket.accept();
      						} catch (IOException e) {
      							e.printStackTrace();
      						}
      					}
      				}
      			});
      			listener.start();
      		} catch (IOException e1) {
      			e1.printStackTrace();
      		}
        }
		
	}

	/**
	 * Monitor postback status
	 */
	private void monitorPostbackStatus() {
		Date oldTime = new Date();
		while (true) {
			Date currentTime = new Date();
			if (currentTime.getTime() / 1000 == oldTime.getTime() / 1000)
				continue;
			if ((currentTime.getTime() - lastPostBackTime.getTime()) % (config.UPLOAD_CYCLE_IN_SECONDS * 1000) == 0) {
				oldTime = currentTime;
				if (postBackStatus)
					LOGGER.info("IoTDB post back sender : postback is in execution!");
			}
		}
	}

	/**
	 * Start postback task in a certain time.
	 */
	public void timedTask() {
		postback();
		lastPostBackTime = new Date();
		Date currentTime = new Date();
		while (true) {
			currentTime = new Date();
			if (currentTime.getTime() - lastPostBackTime.getTime() > config.UPLOAD_CYCLE_IN_SECONDS * 1000) {
				lastPostBackTime = currentTime;
				postback();
			}
		}
	}

	/**
	 * Exceute a postback task.
	 */
	public void postback() {
		if (new File(config.SNAPSHOT_PATH).exists() && new File(config.SNAPSHOT_PATH).list().length != 0) {
			// it means that the last time postback does not succeed! Clear the files and
			// start to postback again
			deleteSnapshot(new File(config.SNAPSHOT_PATH));
		}

		postBackStatus = true;
		connectionOrElse = true;

		// connect to postback server
		getConnection(config.SERVER_IP, config.SERVER_PORT);
		if (!connectionOrElse) {
			LOGGER.info("IoTDB post back sender : postback has failed!");
			postBackStatus = false;
			return;
		}

		FileManager fileManager = FileManager.getInstance();
		fileManager.init();
		Map<String, Set<String>> sendingFileList = fileManager.getSendingFiles();
		Map<String, Set<String>> nowLocalFileList = fileManager.getNowLocalFiles();
		if (isEmpty(sendingFileList)) {
			LOGGER.info("IoTDB post back sender : there has no file to postback !");
			postBackStatus = false;
			return;
		}

		// create snapshot
		for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
			makeFileSnapshot(entry.getValue(), config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
		}

		sendSchema(config.SCHEMA_PATH);
		if (!connectionOrElse) {
			transport.close();
			LOGGER.info("IoTDB post back sender : postback has failed!");
			postBackStatus = false;
			return;
		}
		for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
			Set<String> sendingList = entry.getValue();
			if (sendingList.size() == 0)
				continue;
			LOGGER.info("IoTDB post back sender : postback starts to transfer data of storage group {}.",
					entry.getKey());
			try {
				clientOfServer.init(entry.getKey());
			} catch (TException e) {
				e.printStackTrace();
			}
			startSending(sendingList, config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
			if (!connectionOrElse) {
				transport.close();
				LOGGER.info("IoTDB post back sender : postback has failed!");
				postBackStatus = false;
				return;
			}
			if (afterSending()) {
				nowLocalFileList.get(entry.getKey()).addAll(sendingList);
				fileManager.setNowLocalFiles(nowLocalFileList);
				fileManager.backupNowLocalFileInfo(config.LAST_FILE_INFO);
				if (clearOrNot) {
					deleteData(sendingList, config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
				}
				LOGGER.info("IoTDB post back sender : the postBack has finished storage group {}.", entry.getKey());
			} else {
				LOGGER.info("IoTDB post back sender : postback has failed!");
				postBackStatus = false;
				return;
			}
		}
		deleteSnapshot(new File(config.SNAPSHOT_PATH));
		try {
			clientOfServer.afterReceiving();
		} catch (TException e) {
			e.printStackTrace();
		}
		transport.close();
		LOGGER.info("IoTDB post back sender : the postBack has finished!");
		postBackStatus = false;
		return;
	}
}
