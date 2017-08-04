package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.transferfile.transfer.common.Md5CalculateUtil;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ClientConfig;

import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Date;

/**
 * Created by dell on 2017/7/24.
 */
public class TransferFile extends Thread {
	private Socket socket;
	private String absolutePath;
	private String MD5;
	private long bytePosition;
	private final String messageSplitSig = "\n";
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TransferFile.class);

	public TransferFile(Socket socket, String absolutePath, long bytePosition) {
		this.socket = socket;
		this.absolutePath = absolutePath.substring(0, absolutePath.length());
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
			t = t && MD5.equals(new String(input).split(messageSplitSig)[0]);

			LOGGER.info(new Date().toString() + " ------ finish send file " + new File(absolutePath).getName());

			if (t) {
				deleteFile(absolutePath);
			}
		} catch (IOException e) {
			LOGGER.error("errors occur in socket InputStream or OutputStream!");
		} finally {
			if (ins != null)
				try {
					ins.close();
				} catch (IOException e) {
					LOGGER.error("fail to close socket InputStream!");
				}
		}
	}

	private void sendFileNameAndLength(String absolutePath) throws IOException {
		File file = new File(absolutePath);
		OutputStream os = socket.getOutputStream();
		PrintWriter pw = new PrintWriter(os);
		pw.write(absolutePath + messageSplitSig + file.length() + messageSplitSig + bytePosition + messageSplitSig);
		pw.flush();
		os.flush();
	}

	private boolean writeFileToServer(String absolutePath, Long bytePosition) throws IOException {
		boolean t = true;
		MD5 = Md5CalculateUtil.getFileMD5(absolutePath);

		OutputStream os = null;
		os = socket.getOutputStream();

		File file = new File(absolutePath);
		FileInputStream in = null;
		in = new FileInputStream(file);
		ClientConfig config = ClientConfig.getInstance();
		byte[] buffer = new byte[Math.toIntExact(config.fileSegmentSize)];
		int size = 0;
		Long sendSize = Long.valueOf(0);
		try {
			while ((size = in.read(buffer)) != -1) {
				// System.out.println("sending package,size = " + size);
				sendSize += size;
				if (sendSize <= bytePosition)
					continue;
				os.write(buffer, 0, size);
				os.flush();
				InputStream ins = socket.getInputStream();
				byte[] readAccept = new byte[128];
				ins.read(readAccept);
				String temp = new String(readAccept);
				bytePosition += Long.parseLong(temp.split("\n")[0]);
				updateBytePosition(absolutePath, bytePosition);
			}
		} catch (IOException e) {
			LOGGER.error("errors occur while transferring file " + absolutePath);
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
				LOGGER.error("fail to close FileInputStream while writing file " + absolutePath + " to server!");
			}
		}
		t = (bytePosition == file.length());
		return t;
	}

	private void updateBytePosition(String absolutePath, Long bytePosition) throws IOException {
		ObjectOutputStream oos = null;
		File file = new File(absolutePath);
		ClientConfig config = ClientConfig.getInstance();
		String filePath = config.filePositionRecord.concat("record_" + file.getName());

		try {
			oos = new ObjectOutputStream(new FileOutputStream(filePath));
			oos.writeObject(new FilePositionRecord(absolutePath, bytePosition));
		} catch (FileNotFoundException e) {
			LOGGER.error("Can't find file " + filePath);
		} catch (IOException e) {
			LOGGER.error("write object fail");
		} finally {
			oos.close();
		}
	}

	private static boolean deleteFile(String absolutePath) {
		boolean t = false;
		File file = new File(absolutePath);
		if (file.exists() && file.isFile()) {
			if (file.delete()) {
				System.out.println("delete file " + absolutePath + "success!");
				t = true;
			} else {
				System.out.println("delete file " + absolutePath + "fail!");
				t = false;
			}
		} else {
			System.out.println("delete file fail: " + absolutePath + "not exist!");
			t = false;
		}
		return t;
	}
}