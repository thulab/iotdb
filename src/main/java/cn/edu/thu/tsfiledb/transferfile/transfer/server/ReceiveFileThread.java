package cn.edu.thu.tsfiledb.transferfile.transfer.server;

import cn.edu.thu.tsfiledb.transferfile.transfer.common.Md5CalculateUtil;
import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ServerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * Created by dell on 2017/7/24.
 */
public class ReceiveFileThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveFileThread.class);

	private Socket socket;
	private String receiveFilePath;
	private int fileSize;
	private final String messageSplitSig = "\n";

	public ReceiveFileThread(Socket socket) {
		this.socket = socket;
	}

	/**
	 * write data for communication with
	 * main.java.cn.edu.thu.tsfiledb.transferfile.transfer.client
	 */
	public void run() {
		try {
			readFileFromClient(socket);
			socket.close();
		} catch (IOException e) {
			LOGGER.error("IOException occurs while reading files from client!", e);
		}
	}

	private void readFileFromClient(Socket socket) throws IOException {
		InputStream is = socket.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		byte[] buffer = new byte[1024];
		ServerConfig config = ServerConfig.getInstance();
		String info = null;
		String[] args = new String[5];
		int i = 0;
		while ((i < 3) && (!((info = br.readLine()) == ""))) {
			args[i] = info.substring(0, info.length());
			System.out.println(args[i] + " " + i);
			i++;
		}

		String path = args[0];
		// System.out.println("path "+path);

		fileSize = Integer.parseInt(args[1].substring(0, args[1].length()));
		String[] args1 = path.split(messageSplitSig);
		String fileName = args1[args1.length - 1];

		String temp = config.storageDirectory.concat(new File(fileName).getName());
		receiveFilePath = temp.substring(0, temp.length());
		Long startPosition = Long.parseLong(args[2]);
		// System.out.println("receivePath "+receive_filePath);
		File receive_file = new File(receiveFilePath);
		if (!receive_file.exists()) {
			receive_file.createNewFile();
		}

		FileInputStream fis = new FileInputStream(receive_file);

		File temp_file = new File(config.storageDirectory + "temp_" + new File(fileName).getName());
		FileOutputStream fos = new FileOutputStream(temp_file);

		byte[] copyfile = new byte[128];
		// System.out.println("startPosition "+startPosition);
		int read = 0;
		int total_read = 0;
		while (total_read < startPosition) {
			read = fis.read(copyfile);
			fos.write(copyfile);
			total_read += read;
		}
		fos.close();
		fis.close();

		int tempsize = 0;
		fis = new FileInputStream(temp_file);
		fos = new FileOutputStream(receive_file);
		while (tempsize < startPosition) {
			tempsize += fis.read(copyfile);
			fos.write(copyfile);
		}
		fis.close();

		if (!temp_file.delete()) {
			LOGGER.error("delete file " + temp_file.getAbsoluteFile() + " fail");
		} else {
			LOGGER.info("delete file " + temp_file.getAbsoluteFile() + " success");
		}
		OutputStream ous = socket.getOutputStream();

		PrintWriter pw = new PrintWriter(ous);
		pw.write("ok\n");
		pw.flush();
		ous.flush();
		Long receive_size = startPosition;
		int readSize = 0;

		while ((receive_size < fileSize) && ((readSize = is.read(buffer)) != -1)) {
			receive_size += readSize;
			// System.out.println("server receive_size "+receive_size);
			fos.write(buffer, 0, readSize);
			pw.write(readSize + "\n");
			pw.flush();
			ous.flush();
		}
		LOGGER.info("finish receive a file,sending md5...");
		String md5 = Md5CalculateUtil.getFileMD5(receiveFilePath);
		pw.write(md5 + "\n");
		/** flush OutputStream */
		pw.flush();
		ous.flush();
		/** close streams */
		br.close();
		fos.close();
		isr.close();
		is.close();
		ous.close();
	}
}