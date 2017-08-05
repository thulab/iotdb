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
	private long startPosition;
	private final String messageSplitSig = "\n";

	public ReceiveFileThread(Socket socket) {
		this.socket = socket;
	}

	/**
	 * write data for communication with
	 * main.java.cn.edu.thu.tsfiledb.transferfile.transfer.client
	 */
	public void run() {
		InputStream is=null;
		OutputStream os=null;
		try {
			is = socket.getInputStream();
			os=socket.getOutputStream();
			readFileNameAndLength(is,os);
			receiveFileFromClient(is,os);

		} catch (IOException e) {
			LOGGER.error("IOException occurs while reading files from client!", e);
		} finally{
			try {
				socket.close();/**close InputStream and OutputStream*/
			}catch (IOException e1){
				LOGGER.error("error occur while closing socket!");
			}
		}
	}

	private void readFileNameAndLength(InputStream is,OutputStream os) throws IOException {
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		ServerConfig config = ServerConfig.getInstance();
		String info = null;
		String[] args = new String[5];
		int i = 0;
		while ((i < 3) && (!((info = br.readLine()) == ""))) {
			args[i] = info.substring(0, info.length());
			//System.out.println(args[i] + " " + i);
			i++;
		}

		String path = args[0];
		// System.out.println("path "+path);
		fileSize = Integer.parseInt(args[1].substring(0, args[1].length()));
		String[] args1 = path.split(messageSplitSig);
		String fileName = args1[args1.length - 1];

		String receiveDir=config.storageDirectory;
		File dir=new File(receiveDir);
		if(!dir.exists())dir.mkdir();
		String temp = config.storageDirectory.concat(System.getProperty("file.separator")+new File(fileName).getName());
		receiveFilePath = temp.substring(0, temp.length());
		startPosition = Long.parseLong(args[2]);
		// System.out.println("receivePath "+receive_filePath);
		reWriteReceiveFile(os);
		PrintWriter pw = new PrintWriter(os);
		pw.write("ok\n");
		pw.flush();
		os.flush();
		//pw.close();
	}

	public void reWriteReceiveFile(OutputStream os) throws IOException{
		File receive_file = new File(receiveFilePath);
		//System.out.println("receiveFilePath "+receiveFilePath);
		if (!receive_file.exists()) {
			receive_file.createNewFile();
		}
		ServerConfig config = ServerConfig.getInstance();
		FileInputStream fis = new FileInputStream(receive_file);
		/**copy exist file part from  receive_file to temp_file*/

		File temp_file = new File(config.storageDirectory+System.getProperty("file.separator") + "temp_" + receive_file.getName());
		FileOutputStream fos = new FileOutputStream(temp_file);

		byte[] copyfile = new byte[128];
		//System.out.println("startPosition "+startPosition);
		int read = 0;
		int total_read = 0;
		while (total_read < startPosition) {
			read = fis.read(copyfile);
			fos.write(copyfile);
			total_read += read;
		}
		fos.close();
		fis.close();
		/**copy from temp_file to receive_file*/
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
	}

	private void receiveFileFromClient(InputStream is, OutputStream os) throws IOException {
		long receive_size = startPosition;
		int readSize = 0;
		FileOutputStream fos=new FileOutputStream(receiveFilePath);
		PrintWriter pw = new PrintWriter(os);
		byte[] buffer = new byte[1024];
		while ((receive_size < fileSize) && ((readSize = is.read(buffer)) != -1)) {
			receive_size += readSize;
			// System.out.println("server receive_size "+receive_size);
			fos.write(buffer, 0, readSize);
			pw.write(readSize + "\n");
			pw.flush();
			os.flush();
		}
		LOGGER.info("finish receive a file,sending md5...");
		String md5 = Md5CalculateUtil.getFileMD5(receiveFilePath);
		pw.write(md5 + "\n");
		/** flush OutputStream */
		pw.flush();
		os.flush();
		pw.close();
	}
}