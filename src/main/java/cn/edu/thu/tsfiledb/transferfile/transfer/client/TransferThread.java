package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.service.DataCollectClient;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileInfo;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameAllResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ClientConfig;

import org.apache.commons.io.IOUtils;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dell on 2017/7/25.
 */
public class TransferThread extends TimerTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransferThread.class);
	private static ClientConfig config = ClientConfig.getInstance();
	private final int copyFileSegment=1024;

	public void run() {
		File dir = new File(config.snapshotDirectory);
		if (!dir.exists())
			MakeDir(dir.getAbsolutePath());
		File[] files = dir.listFiles();
		if (Client.isTimerTaskRunning() && files.length > 0) {
			LOGGER.warn("Still transferring");
			return;
		}
		Client.setTimerTaskRunning(true);
		try {
			if (files.length == 0) {
				getFileFromDB();
			}
			LOGGER.info(" ------ transfer files");
			writeFilesToServer(config.snapshotDirectory);
		} catch (IOException e) {
			LOGGER.error("Errors occur in TransferThread: Not finish copying files to snapshot "
					+ "directory {}!", config.snapshotDirectory, e);
		}
	}

	public void getFileFromDB() throws IOException {
		DataCollectClient client;
		try {
			client = new DataCollectClient(config.readDBHost, config.readDBPort);
		} catch (TTransportException e) {
			LOGGER.error("Failed to init data collect client, because: {}", e.getMessage());
			return;
		}
		try {
			TSFileNodeNameAllResp tsFileNodeNameAllResp = client.getFileAllNode();
			List<String> fileNodeList = tsFileNodeNameAllResp.getFileNodesList();

			for (String namespace: fileNodeList) {
				Map<String, Long> startTimes = loadStartTimes(namespace);
				
				// show start times for every device in nameSpace
				// the following codes are only used for debug
				StringBuilder startTimeInfo = new StringBuilder();
				startTimeInfo.append("show start times\n");
				for (Map.Entry<String, Long> entry : startTimes.entrySet()) {
					startTimeInfo.append("\t")
					.append(entry.getKey())
					.append(" start time: ")
					.append(entry.getValue())
					.append("\n");
				}
				//System.out.println(startTimeInfo.toString());
				
				TSFileNodeNameResp tsFileNodeNameResp = client.getFileNode(namespace, startTimes, System.currentTimeMillis());
				int token = tsFileNodeNameResp.getToken();
				List<TSFileInfo> tsFileInfoList = tsFileNodeNameResp.getFileInfoList();// tsFileInfo

				for (TSFileInfo info : tsFileInfoList) {
					String tsFilePath = info.getFilePath();// device			
					copyFileSnapShot(tsFilePath, config.snapshotDirectory); // dir
					updateStartTimes(namespace, info.getEndTimes());
				}
				client.backFileNode(namespace, tsFileInfoList, token);
			}
		} finally {
			client.close();
		}
	}

	private void copyFileSnapShot(String tsFilePath, String snapShotPath) throws IOException {
		LOGGER.info("Copy file from {} to {}...", tsFilePath, snapShotPath);
		File inputFile = new File(tsFilePath);
		//String snapShotDir = snapShotPath.concat(File.separatorChar);
		//File snapShotDirFile=new File(snapShotDir);
		//if(!snapShotDirFile.exists())MakeDir(snapShotDir);
		File outputFile = new File(snapShotPath.concat(File.separatorChar + inputFile.getName()));
		FileInputStream fis = null;
		FileOutputStream fos = null;
		try {
			fis = new FileInputStream(inputFile);
			fos = new FileOutputStream(outputFile);
			byte[] copyfile = new byte[copyFileSegment];
			while (fis.read(copyfile) != -1) {
				fos.write(copyfile);
			}
		} catch (FileNotFoundException e) {
			LOGGER.error("No file to copy, because: {}", e.getMessage());
		} finally {
			try {
				if (fis != null)
					fis.close();
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				LOGGER.error("Fail to close file after copy, because: {}", e.getMessage());
			}
		}
	}

	private void updateStartTimes(String namespace, Map<String, Long> newStartTime) {
		ObjectOutputStream oos = null;
		String dirPath = config.startTimePath.concat(File.separatorChar + namespace + File.separatorChar);

		for (Map.Entry<String, Long> entry : newStartTime.entrySet()) {
			try {
				String filePath = dirPath.concat(entry.getKey());
				File dir = new File(dirPath);
				if (!dir.exists())
					dir.mkdirs();
				oos = new ObjectOutputStream(new FileOutputStream(filePath));
				oos.writeObject(new StartTime(entry.getKey(), entry.getValue() + 1));
			} catch (IOException e) {
				LOGGER.error("Update startTime for {} fail, because: {}", entry.getKey(), e.getMessage());
			} finally {
				IOUtils.closeQuietly(oos);
			}
		}
	}

	private Map<String, Long> loadStartTimes(String namespace) {
		Map<String, Long> startTimes = new HashMap<>();
		String path = config.startTimePath.concat(File.separatorChar + namespace);
		File dir = new File(path);
		if(!dir.exists()){
			MakeDir(dir.getAbsolutePath());
		}
		ObjectInputStream ois = null;
		if (dir.exists()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				try {
					ois = new ObjectInputStream(new FileInputStream(file));
					StartTime startTime = (StartTime) ois.readObject();
					startTimes.put(startTime.getDevice(), startTime.getStartTime());
				} catch (FileNotFoundException e) {
					LOGGER.error("Can't find file {}", file.getName());
				} catch (IOException | ClassNotFoundException e) {
					startTimes.put(file.getName(), 0L);
					LOGGER.warn("file {}: reset startTime to 0, because: {}", file.getName(), e.getMessage());
				} finally {
					if (ois != null)
						try {
							ois.close();
						} catch (IOException e) {
							LOGGER.error("fail to close file {}, becasuse: {}", file.getName(), e.getMessage());
						}
				}
			}
		}
		return startTimes;
	}

	private void writeFilesToServer(String path) {
		File file = new File(path);
		File[] files = file.listFiles();

		while (file.exists() && files.length > 0) {
			// thread interruption,file re_transfer
			ExecutorService fixedThreadPool = Executors.newFixedThreadPool(config.clientNTread);
			for (File traverseFile : files) {
				LOGGER.info(" ------ transfer a file {}", traverseFile.getName());
				try {
					Socket socket = new Socket(config.serverAddress, config.port);
					LOGGER.info("socket success for fileDir {}", path);

					fixedThreadPool.submit(new TransferFileThread(socket, traverseFile.getAbsolutePath(),
							getFileBytePosition(traverseFile.getAbsolutePath())));
				} catch (UnknownHostException e) {
					LOGGER.error("build socket error, because: {}", e.getMessage());
				} catch (IOException e) {
					LOGGER.error("build socket error, because: {}", e.getMessage());
				}
			}
			fixedThreadPool.shutdown();

			while (!fixedThreadPool.isTerminated()) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
			fixedThreadPool.shutdownNow();
			file = new File(path);
			files = file.listFiles();
		}
	}

	private long getFileBytePosition(String filePath) {
		long bytePosition = 0;
		File file = new File(filePath);
		File dir = new File(config.filePositionRecord+File.separatorChar);
		LOGGER.info("fileRecord Dir "+dir.getAbsolutePath());
		if(!dir.exists())
			MakeDir(dir.getAbsolutePath());
		String fileRecordPath = config.filePositionRecord.concat(File.separatorChar + "record_" + file.getName());

		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		File recordFile = new File(fileRecordPath);

		if (recordFile.exists()) {
			try {
				ois = new ObjectInputStream(new FileInputStream(recordFile));
				FilePositionRecord filePositionRecord = (FilePositionRecord) ois.readObject();
				return filePositionRecord.getBytePosition();
			} catch (FileNotFoundException e) {
				LOGGER.error("Can't find record file for file {}, reset bytePosition to 0", file.getName());
				return 0;
			} catch (IOException | ClassNotFoundException e) {
				try {
					oos = new ObjectOutputStream(new FileOutputStream(recordFile));
					oos.writeObject(new FilePositionRecord(file.getAbsolutePath(), 0L));
				} catch (IOException e1) {
					LOGGER.error("Fail to rewrite recordFile, because: {}", e1.getMessage());
				}
				LOGGER.info("File {}, reset bytePosition to 0", file.getName());
				return 0L;
			} finally {
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
					}
				}
				if(ois != null){
					try {
						ois.close();
					} catch (IOException e) {
					}
				}
			}
		} else {
			try {
				//LOGGER.info("recordFile "+recordFile.getAbsolutePath());
				recordFile.createNewFile();
				oos = new ObjectOutputStream(new FileOutputStream(recordFile));
				oos.writeObject(new FilePositionRecord(file.getAbsolutePath(), 0L));
			} catch (IOException e) {
				LOGGER.error("Fail to rewrite recordFile, because: {}", e.getMessage());
			} finally {
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
					}
				}
				if(ois != null){
					try {
						ois.close();
					} catch (IOException e) {
					}
				}
			}
		}
		return bytePosition;
	}

	private void MakeDir(String absolutePath) {
		File dir=new File(absolutePath);
		if(!dir.exists()){
			MakeDir(dir.getParent());
			dir.mkdir();
		}
	}
}