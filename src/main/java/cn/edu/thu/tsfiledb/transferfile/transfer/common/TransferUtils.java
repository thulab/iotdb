package cn.edu.thu.tsfiledb.transferfile.transfer.common;

import cn.edu.thu.tsfiledb.transferfile.transfer.sender.TransferThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TransferUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransferThread.class);
	public static boolean deleteFile(String absolutePath) {
		boolean t = false;
		File file = new File(absolutePath);
		if (file.exists() && file.isFile()) {
			if (file.delete()) {
				LOGGER.info("delete file " + absolutePath + " success!");
				t = true;
			} else {
				LOGGER.error("delete file " + absolutePath + " fail!");
				t = false;
			}
		} else {
			LOGGER.warn("delete file fail: " + absolutePath + " not exist!");
			t = false;
		}
		return t;
	}
}
