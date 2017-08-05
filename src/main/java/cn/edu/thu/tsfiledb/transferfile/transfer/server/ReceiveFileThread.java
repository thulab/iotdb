package cn.edu.thu.tsfiledb.transferfile.transfer.server;

import cn.edu.thu.tsfiledb.transferfile.transfer.common.Md5CalculateUtil;
import cn.edu.thu.tsfiledb.transferfile.transfer.common.TransferConstants;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ServerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by dell on 2017/7/24.
 */
public class ReceiveFileThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveFileThread.class);

	private Socket socket;
	private String receiveFilePath;
	private long fileSize;
	private long startPosition;

	public ReceiveFileThread(Socket socket) {
		this.socket = socket;
	}

	/**
	 * write data for communication with
	 * main.java.cn.edu.thu.tsfiledb.transferfile.transfer.client
	 */
	public void run() {
		InputStream is = null;
		OutputStream os = null;
		try {
			is = socket.getInputStream();
			os = socket.getOutputStream();
			readFileNameAndLength(is, os);
			receiveFileFromClient(is, os);

		} catch (IOException e) {
			LOGGER.error("Error occurs while reading files from client, because: {}", e.getMessage());
		} finally {
			try {
				// close InputStream and OutputStream
				socket.close();
			} catch (IOException e1) {
				LOGGER.error("Error occurs while closing socket!", e1);
			}
		}
	}

	private void readFileNameAndLength(InputStream is, OutputStream os) throws IOException {
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		ServerConfig config = ServerConfig.getInstance();
		String info = null;
		String[] args = new String[5];
		int i = 0;
		while ((i < 3) && (!((info = br.readLine()) == ""))) {
			args[i] = info;
			i++;
		}

		String path = args[0];
		fileSize = Long.parseLong(args[1]);
		String[] values = path.split(TransferConstants.messageSplitSig);
		String fileName = values[values.length - 1];

		String receiveDir = config.storageDirectory;
		File dir = new File(receiveDir);
		if (!dir.exists())
			dir.mkdir();
		
		String temp = config.storageDirectory.concat(File.separatorChar + new File(fileName).getName());
		receiveFilePath = temp;
		startPosition = Long.parseLong(args[2]);
		rewriteReceiveFile(os);
		PrintWriter pw = new PrintWriter(os);
		pw.write("ok\n");
		pw.flush();
		os.flush();
	}

	private void rewriteReceiveFile(OutputStream os) throws IOException {
		File receiveFile = new File(receiveFilePath);
		if (!receiveFile.exists()) {
			receiveFile.createNewFile();
		}
		ServerConfig config = ServerConfig.getInstance();
		File tempFile = new File(config.storageDirectory + File.separator + "temp_" + receiveFile.getName());
		FileInputStream fis = null;
		FileOutputStream fos = null;
		byte[] copyfile = new byte[128];
		try {
			// copy exist file part from receiveFile to tempFile
			fis = new FileInputStream(receiveFile);
			fos = new FileOutputStream(tempFile);
			int read = 0;
			int totalRead = 0;
			while (totalRead < startPosition) {
				read = fis.read(copyfile);
				fos.write(copyfile);
				totalRead += read;
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if(fos != null) fos.close();
			if(fis != null) fis.close();
		}
		
		try {
			// copy from tempFile to receiveFile
			int tempsize = 0;
			fis = new FileInputStream(tempFile);
			fos = new FileOutputStream(receiveFile);
			while (tempsize < startPosition) {
				tempsize += fis.read(copyfile);
				fos.write(copyfile);
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if(fos != null) fos.close();
			if(fis != null) fis.close();
		}

		if (!tempFile.delete()) {
			LOGGER.error("delete file {} fail", tempFile.getAbsoluteFile());
		} else {
			LOGGER.info("delete file {} success", tempFile.getAbsoluteFile());
		}
	}

	private void receiveFileFromClient(InputStream is, OutputStream os) throws IOException {
		long receiveSize = startPosition;
		int readSize = 0;
		FileOutputStream fos = new FileOutputStream(receiveFilePath);
		PrintWriter pw = new PrintWriter(os);
		try {
			byte[] buffer = new byte[1024];
			while ((receiveSize < fileSize) && ((readSize = is.read(buffer)) != -1)) {
				receiveSize += readSize;
				fos.write(buffer, 0, readSize);
				pw.write(readSize + "\n");
				pw.flush();
				os.flush();
			}
			LOGGER.info("Finish receiving a file, sending md5...");
			String md5 = Md5CalculateUtil.getFileMD5(receiveFilePath);
			pw.write(md5 + "\n");
			
			// flush OutputStream
			pw.flush();
			os.flush();
		} finally {
			if(pw != null){
				pw.close();
			}
			if(fos != null){
				fos.close();
			}
		}

	}
}