package cn.edu.tsinghua.postback.iotdb.sender;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.postback.iotdb.receiver.ServerManager;
import cn.edu.tsinghua.postback.iotdb.receiver.Service;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackDescriptor;

public class TransferData {

	private TTransport transport;
	private Service.Client clientOfServer;
	private List<String> schema = new ArrayList<>();
	private String uuid;
	private boolean connection_orElse;
	private PostBackConfig config = PostBackDescriptor.getInstance().getConfig();

	private static final Logger LOGGER = LoggerFactory.getLogger(TransferData.class);

	private static class TransferHolder {
		private static final TransferData INSTANCE = new TransferData();
	}

	private TransferData() {
	}

	public static final TransferData getInstance() {
		return TransferHolder.INSTANCE;
	}

	public void setConfig(PostBackConfig config) {
		this.config = config;
	}

	public void connection(String serverIP, int serverPort) {
		transport = new TSocket(serverIP, serverPort);
		TProtocol protocol = new TBinaryProtocol(transport);
		clientOfServer = new Service.Client(protocol);
		try {
			transport.open();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back sender: IoTDB post back sender: cannot connect to server because {}",
					e.getMessage());
			connection_orElse = false;
		}
	}

	public String transferUUID(String uuidPath) {
		String uuidOfReceiver = null;
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
				connection_orElse = false;
			}
		} else {
			try {
				bf = new BufferedReader(new FileReader(uuidPath));
				uuid = bf.readLine();
				bf.close();
			} catch (IOException e) {
				LOGGER.error("IoTDB post back sender: cannot read UUID from file because {}", e.getMessage());
				connection_orElse = false;
			}
		}
		try {
			uuidOfReceiver = clientOfServer.getUUID(uuid);
		} catch (TException e) {
			LOGGER.error("IoTDB sender: cannot send UUID to receiver because {}", e.getMessage());
			connection_orElse = false;
		}
		return uuidOfReceiver;
	}

	public void makeFileSnapshot(Set<String> sendingFileList, String snapshotPath, String iotdbPath) {
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

	public void startSending(Set<String> fileList, String snapshotPath, String iotdbPath) {
		try {
			int num = 0;
			for (String filePath : fileList) {
				num++;
				String relativeFilePath = filePath.substring(iotdbPath.length());
				String dataPath = snapshotPath + File.separator + relativeFilePath;
				File file = new File(dataPath);
				filePath = filePath.substring(iotdbPath.length());
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
						LOGGER.info("IoTDB sender: receiver has received {} {}", filePath, "successfully!");
						break;
					}
				}
				LOGGER.info("IoTDB sender : Task of sending files to receiver has completed " + num + "/"
						+ fileList.size());
			}
		} catch (Exception e) {
			LOGGER.error("IoTDB sender: cannot sending data because {}", e.getMessage());
			connection_orElse = false;
		}
	}

	public void sendSchema(String schemaPath) {
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
				clientOfServer.getSchema(buffToSend, 1);
			}
			bos.close();
			fis.close();
			clientOfServer.getSchema(null, 0);
		} catch (Exception e) {
			LOGGER.error("IoTDB sender : cannot send schema from mlog.txt because {}", e.getMessage());
			connection_orElse = false;
		}
	}

	public void stop() {
		// TODO Auto-generated method stub

	}

	public boolean afterSending(String snapshotPath) {
		deleteSnapshot(new File(snapshotPath));
		try {
			if(clientOfServer.afterReceiving())
				return true;
			else
				return false;
		} catch (TException e) {
			LOGGER.error("IoTDB sender : can not close connection to the receiver because {}", e.getMessage());
		} finally {
			transport.close();
		}
		return true;
	}

	public void deleteSnapshot(File file) {
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

	public List<String> getSchema() {
		return schema;
	}

	public String getUuid() {
		return uuid;
	}

	public static void main(String[] args) {
		TransferData transferData = new TransferData();
		transferData.timedTask();
	}

	public void timedTask() {
		if (!config.IS_UPLOAD_ENABLE) {
			LOGGER.info(
					"IoTDB post back sender: the iotdb-postback property IS_UPLOAD_ENABLE is set false, so poatback function is banned. ");
		} else {
			postback();
			Date lastTime = new Date();
			Date currentTime = new Date();
			while (true) {
				currentTime = new Date();
				if (currentTime.getTime() - lastTime.getTime() > config.UPLOAD_CYCLE_IN_SECONDS * 1000) {
					postback();
					lastTime = currentTime;
				}
			}
		}
	}

	public void postback() {
		try {
			Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
			Connection connection = null;
			try {
				connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
				Statement statement = connection.createStatement();
				statement.execute("flush");
				statement.execute("merge");
				statement.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (connection != null) {
					connection.close();
				}
			}
		} catch (ClassNotFoundException | SQLException e) {
			LOGGER.error("IoTDB post back sender: cannot execute flush and merge because {}", e.getMessage());
		}
		if (new File(config.SNAPSHOT_PATH).exists() && new File(config.SNAPSHOT_PATH).list().length != 0) {
			// it means that the last time postback does not succeed! Clear the files and
			// start to postback again
			deleteSnapshot(new File(config.SNAPSHOT_PATH));
		}
		FileManager fileManager = FileManager.getInstance();
		fileManager.getLastLocalFileList(config.LAST_FILE_INFO);
		fileManager.getNowLocalFileList(config.SENDER_FILE_PATH);
		fileManager.getSendingFileList();
		Set<String> sendingList = fileManager.getSendingFiles();
		connection_orElse = true;
		if (sendingList.size() == 0)
			LOGGER.info("IoTDB post back sender : there has no files to postback!");
		else {
			connection(config.SERVER_IP, config.SERVER_PORT);
			if (!connection_orElse)
				return;
			transferUUID(config.UUID_PATH);
			if (!connection_orElse) {
				transport.close();
				return;
			}
			makeFileSnapshot(sendingList, config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
			sendSchema(config.SCHEMA_PATH);
			if (!connection_orElse) {
				transport.close();
				return;
			}
			startSending(sendingList, config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
			if (!connection_orElse) {
				transport.close();
				return;
			}
			if(afterSending(config.SNAPSHOT_PATH)){
				fileManager.backupNowLocalFileInfo(config.LAST_FILE_INFO);
			}
			else {
				LOGGER.info("IoTDB post back sender : receiver has broken down!");
			}
		}
	}
}