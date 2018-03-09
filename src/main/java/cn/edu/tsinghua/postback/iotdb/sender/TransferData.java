package cn.edu.tsinghua.postback.iotdb.sender;
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
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;

public class TransferData {

	private TTransport transport;
	private Service.Client clientOfServer;
	private List<String> schema = new ArrayList<>();
	private String uuid;//Mark the identity of sender
	private boolean connection_orElse; // Mark whether connection of sender and receiver has broken down or not
	private PostBackSenderConfig config = PostBackSenderDescriptor.getInstance().getConfig();
	private Date lastPostBackTime = new Date(); // Mark the start time of last postback 
	private boolean PostBackStatus = false; // If true, postback is in execution.

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

	/**
	 * Eatablish a connection between sender and receiver
	 * @param serverIP
	 * @param serverPort:it must be same with port receiver set.
	 */
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

	/**
	 * UUID marks the identity of sender for receiver. 
	 */
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

	/**
	 * Create snapshots for those sending files.
	 */
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

	public boolean afterSending(String snapshotPath) {
		deleteSnapshot(new File(snapshotPath));
		boolean successOrNot = false;
		try {
			successOrNot = clientOfServer.afterReceiving();
		} catch (TException e) {
			LOGGER.error("IoTDB sender : can not finish postback because postback receiver has broken down!");
		} finally {
			transport.close();
		}
		return successOrNot;
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
		Thread monitor = new Thread(new Runnable() {
			public void run() {
				transferData.monitorPostbackStatus();
			}
		});
		monitor.start();
		transferData.timedTask();
	}

	/**
	 * Monitor postback status
	 */
	void monitorPostbackStatus() {
		Date oldTime = new Date();
		while (true) {
			Date currentTime = new Date();
			if (currentTime.getTime() / 1000 == oldTime.getTime() / 1000)
				continue;
			if ((currentTime.getTime() - lastPostBackTime.getTime()) % (config.UPLOAD_CYCLE_IN_SECONDS * 1000) == 0) {
				oldTime = currentTime;
				if (PostBackStatus)
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

	public void postback() {
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
			LOGGER.info("IoTDB post back sender : there has no files to postback !");
		else {
			PostBackStatus = true;
			connection(config.SERVER_IP, config.SERVER_PORT);
			if (!connection_orElse) {
				LOGGER.info("IoTDB post back sender : postback failed!");
				PostBackStatus = false;
				return;
			}
			transferUUID(config.UUID_PATH);
			if (!connection_orElse) {
				transport.close();
				LOGGER.info("IoTDB post back sender : postback failed!");
				PostBackStatus = false;
				return;
			}
			makeFileSnapshot(sendingList, config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
			sendSchema(config.SCHEMA_PATH);
			if (!connection_orElse) {
				transport.close();
				LOGGER.info("IoTDB post back sender : postback failed!");
				PostBackStatus = false;
				return;
			}
			startSending(sendingList, config.SNAPSHOT_PATH, config.IOTDB_DATA_DIRECTORY);
			if (!connection_orElse) {
				transport.close();
				LOGGER.info("IoTDB post back sender : postback failed!");
				PostBackStatus = false;
				return;
			}
			if (afterSending(config.SNAPSHOT_PATH)) {
				fileManager.backupNowLocalFileInfo(config.LAST_FILE_INFO);
				LOGGER.info("IoTDB post back sender : the postBack has finished!");
			} else {
				LOGGER.info("IoTDB post back sender : postback failed!");
				PostBackStatus = false;
				return;
			}
		}
		PostBackStatus = false;
		return;
	}
}
