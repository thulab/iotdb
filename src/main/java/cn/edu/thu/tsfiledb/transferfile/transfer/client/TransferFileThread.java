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
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TransferFileThread.class);

	public TransferFileThread(Socket socket, String absolutePath, long bytePosition) {
		this.socket = socket;
		this.absolutePath = absolutePath;
		this.bytePosition = bytePosition;
	}

	public void run() {
		InputStream ins = null;
		try {
			ins = socket.getInputStream();
			sendFileNameAndLength(absolutePath);
			byte[] input = new byte[1024];
			ins.read(input);
			boolean t = writeFileToServer(absolutePath, bytePosition);
			ins.read(input);
			t = t && MD5.equals(new String(input).split(TransferConstants.messageSplitSig)[0]);

			LOGGER.info("Finish send file {}", new File(absolutePath).getName());

			if (t) {
				TransferUtils.deleteFile(absolutePath);
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
		pw.write(absolutePath + TransferConstants.messageSplitSig +
				file.length() + TransferConstants.messageSplitSig +
				bytePosition + TransferConstants.messageSplitSig);
		pw.flush();
		os.flush();
	}

	private boolean writeFileToServer(String absolutePath, long bytePosition) throws IOException {
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
				if (sendSize <= bytePosition)
					continue;
				os.write(buffer, 0, size);
				os.flush();
				InputStream ins = socket.getInputStream();
				byte[] readAccept = new byte[128];
				ins.read(readAccept);
				String temp = new String(readAccept);
				bytePosition += Long.parseLong(temp.split(TransferConstants.messageSplitSig)[0]);
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