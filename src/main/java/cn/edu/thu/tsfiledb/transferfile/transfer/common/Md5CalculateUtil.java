package cn.edu.thu.tsfiledb.transferfile.transfer.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lylw on 2017/7/22.
 */
public class Md5CalculateUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(Md5CalculateUtil.class);
	private static char[] hexChar = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	private static String toHexString(byte[] b) {
		StringBuilder sbr = new StringBuilder(b.length * 2);
		for (int i = 0; i < b.length; i++) {
			sbr.append(hexChar[(b[i] & 0xf0) >>> 4]);
			sbr.append(hexChar[b[i] & 0x0f]);
		}
		return sbr.toString();
	}

	public static String getFileMD5(String absolutePath) {
		File file = new File(absolutePath);
		String md5 = null;
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(file);
			MessageDigest MD5 = MessageDigest.getInstance("MD5");
			byte[] buffer = new byte[1024];
			int length;
			try {
				while ((length = fileInputStream.read(buffer)) != -1) {
					MD5.update(buffer, 0, length);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			md5 = toHexString(MD5.digest());
		} catch (FileNotFoundException e) {
			return null;
		} catch (NoSuchAlgorithmException e) {
			return null;
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					LOGGER.error("fail to close FileInputStream after calculating MD5!",e);
				}
			}
		}
		return md5;
	}
}