package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.transferfile.transfer.common.Md5CalculateUtil;
import cn.edu.thu.tsfiledb.transferfile.transfer.common.TransferConstants;
import cn.edu.thu.tsfiledb.transferfile.transfer.common.TransferUtils;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ClientConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by dell on 2017/7/24.
 */
public class TransferFileThread extends Thread {
	private Socket socket;
	private String absolutePath;
	private String MD5;
	private long bytePosition;
	private int fileSegmentSize;
	private static final Logger LOGGER = LoggerFactory.getLogger(TransferFileThread.class);
	private final int messageSize=1024;
	private static ClientConfig config = ClientConfig.getInstance();
	public TransferFileThread(Socket socket, String absolutePath, long bytePosition) {
		fileSegmentSize = config.fileSegmentSize;
		this.socket = socket;
		this.absolutePath = absolutePath;
		this.bytePosition = (bytePosition/fileSegmentSize)*fileSegmentSize;
	}

	public void run() {
		InputStream ins = null;
		try {
			ins = socket.getInputStream();
			LOGGER.info("Start sending file");
			sendFileNameAndLength(absolutePath);
			byte[] input = new byte[messageSize];
			ins.read(input);
			LOGGER.info("Receive ok from server");
			boolean t = writeFileToServer(absolutePath, bytePosition);
			LOGGER.info("Ready to read Md5 from server");
			ins.read(input);
			LOGGER.info("Finish reading MD5");
			String serverMD5 = new String(input).split(TransferConstants.messageSplitSig)[0];
			if(MD5!=null) t = t && MD5.equals(serverMD5);
			else t=false;
			LOGGER.info("Finish send file {}", new File(absolutePath).getName());

			if (t) {
				TransferUtils.deleteFile(absolutePath);
			}
			else{
				updateBytePosition(absolutePath,0L);
			}
		} catch (IOException e) {
			LOGGER.error("Errors occur in socket InputStream or OutputStream!", e);
		} finally {
			if (ins != null){
				try {
					ins.close();
				} catch (IOException e) {
					LOGGER.error("Fail to close socket!", e);
				}
			}

		}
	}

	private void sendFileNameAndLength(String absolutePath) throws IOException {
		File file = new File(absolutePath);
		OutputStream os = socket.getOutputStream();
		PrintWriter pw = new PrintWriter(os);
		pw.write(file.getName() + TransferConstants.messageSplitSig +
				file.getParentFile().getName() + TransferConstants.messageSplitSig +
				file.length() + TransferConstants.messageSplitSig +
				bytePosition + TransferConstants.messageSplitSig);
		pw.flush();
		os.flush();
	}

	private boolean writeFileToServer(String absolutePath, long bytePosition) throws IOException {
		LOGGER.info("start send file size: " + bytePosition);
		boolean t = true;
		MD5 = Md5CalculateUtil.getFileMD5(absolutePath);
		OutputStream os = socket.getOutputStream();
		File file = new File(absolutePath);
		FileInputStream in = new FileInputStream(new File(absolutePath));
		ClientConfig config = ClientConfig.getInstance();
		byte[] buffer = new byte[Math.toIntExact(config.fileSegmentSize)];
		int size = 0;
		long sendSize = 0;

		try {
			while ((size = in.read(buffer)) != -1) {
				sendSize += size;
				LOGGER.info("Client send size "+sendSize);
				if (sendSize <= bytePosition)
					continue;
				os.write(buffer, 0, size);
				os.flush();
				InputStream ins = socket.getInputStream();
				byte[] readAccept = new byte[messageSize];
				ins.read(readAccept);
				String temp = new String(readAccept);
				bytePosition = Long.parseLong(temp.split(TransferConstants.messageSplitSig)[0]);
				LOGGER.info("file size: " + bytePosition);
				updateBytePosition(absolutePath, bytePosition);
			}
		} catch (IOException e) {
			LOGGER.error("Errors occur while transferring file {}", absolutePath, e);
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
				LOGGER.error("Fail to close FileInputStream while writing file {} to server, because: ", absolutePath, e.getMessage());
			}
		}
		t = (bytePosition == file.length());
		return t;
	}

	private void updateBytePosition(String absolutePath, Long bytePosition) {
		ObjectOutputStream oos = null;
		File file = new File(absolutePath);
		ClientConfig config = ClientConfig.getInstance();
		String filePath = config.filePositionRecord.concat(File.separatorChar+"record_" + file.getName());

		try {
			oos = new ObjectOutputStream(new FileOutputStream(filePath));
			oos.writeObject(new FilePositionRecord(absolutePath, bytePosition));
		} catch (FileNotFoundException e) {
			LOGGER.error("Can't find file {}", filePath);
		} catch (IOException e) {
			LOGGER.error("write object fail because: {}", e.getMessage());
		} finally {
			if(oos != null){
				try {
					oos.close();
				} catch (IOException e) {
				}
			}
		}
	}
}