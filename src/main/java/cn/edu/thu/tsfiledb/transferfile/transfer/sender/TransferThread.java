package cn.edu.thu.tsfiledb.transferfile.transfer.sender;

import cn.edu.thu.tsfiledb.service.DataCollectClient;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileInfo;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameAllResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.SenderConfig;

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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dell on 2017/7/25.
 */
public class TransferThread extends TimerTask {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransferThread.class);
	private static SenderConfig config = SenderConfig.getInstance();
	private final int copyFileSegment=1024;

	public void run() {
		File dir = new File(config.snapshotDirectory);
		if (!dir.exists())
			dir.mkdirs();
		File[] files = getAllFileInDir(dir);
		if (Sender.isTimerTaskRunning() && files.length > 0) {
			LOGGER.warn("Still transferring");
			return;
		}
		Sender.setTimerTaskRunning(true);
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
			LOGGER.error("Failed to init data collect sender, because: {}", e.getMessage());
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
		String snapShotDir = snapShotPath.concat(File.separatorChar + inputFile.getParentFile().getName() + File.separatorChar);
		//LOGGER.info("getParentFile.getName "+inputFile.getParent());
		File snapShotDirFile=new File(snapShotDir);
		if(!snapShotDirFile.exists())snapShotDirFile.mkdirs();
		File outputFile = new File(snapShotDir + inputFile.getName());
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
		Map<String,Long> startTimes = new HashMap<>();
		File dirFile = new File(config.startTimePath);
		if(!dirFile.exists()) dirFile.mkdirs();
		File file = new File(config.startTimePath.concat(File.separatorChar + namespace));
		if(!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				LOGGER.warn("Fail to create start-time file {}",file.getName());
			}
		}
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(file));
			startTimes.putAll(((StartTime)ois.readObject()).getStartTime());

		} catch (ClassNotFoundException e) {
			LOGGER.error("Can't find file {}", file.getName());
		} catch (FileNotFoundException e) {
			LOGGER.error("Can't find file {}", file.getName());
		} catch (IOException e) {
			LOGGER.error("Can't read start time from file {}",file.getName());
		} finally {
			IOUtils.closeQuietly(ois);
		}
		ObjectOutputStream oos = null;
		for (Map.Entry<String, Long> entry : newStartTime.entrySet()) {
			startTimes.put(entry.getKey(),entry.getValue());
		}
		try {
			oos = new ObjectOutputStream(new FileOutputStream(file));
			oos.writeObject(new StartTime(namespace,startTimes));
		} catch (IOException e) {
			LOGGER.error("Update startTime for {} fail, because: {}", namespace, e.getMessage());
		}
	}

	private Map<String, Long> loadStartTimes(String namespace) {
		Map<String, Long> startTimes = new HashMap<>();
		File dirPath = new File(config.startTimePath);
		if(!dirPath.exists())dirPath.mkdirs();
		String filePath = config.startTimePath.concat(File.separatorChar + namespace);
		File file = new File(filePath);
		if(!file.exists()){
			return startTimes;
		}
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(file));
			StartTime startTime = (StartTime) ois.readObject();
			startTimes.putAll(startTime.getStartTime());
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.warn("file {}: reset startTime to empty, because: {}", file.getName(), e.getMessage());
		} catch (ClassNotFoundException e) {
			LOGGER.error("Can't find file {}", file.getName());
		} finally{
			if (ois != null)
				try {
					ois.close();
				} catch (IOException e) {
					LOGGER.error("fail to close file {}, becasuse: {}", file.getName(), e.getMessage());
				}
		}
		return startTimes;
	}

	private void writeFilesToServer(String path) {
		File file = new File(path);
		File[] files = getAllFileInDir(file);

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
			files = getAllFileInDir(file);
		}
	}

	private File[] getAllFileInDir(File file) {
		Queue<File> dirQueue = new LinkedList<>();
		File[] dirArray = file.listFiles();
		List<File> fileList = new ArrayList<>();
		for (File dir : dirArray){
			dirQueue.add(dir);
		}
		while(!dirQueue.isEmpty()){
			File f = dirQueue.remove();
			if (f.isFile())fileList.add(f);
			else {
				File[] files = f.listFiles();
				for (File qFile : files) dirQueue.add(qFile);
			}
		}
		File[] returnList = new File[fileList.size()];
		for (int index = 0;index <fileList.size();index++) returnList[index] = fileList.get(index);
		return returnList;
	}

	private long getFileBytePosition(String filePath) {
		long bytePosition = 0;
		File file = new File(filePath);
		File dir = new File(config.filePositionRecord+File.separatorChar);
		LOGGER.info("fileRecord Dir "+dir.getAbsolutePath());
		if(!dir.exists())
			dir.mkdirs();
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