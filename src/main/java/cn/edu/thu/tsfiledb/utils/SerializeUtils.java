package cn.edu.thu.tsfiledb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

import org.apache.derby.iapi.store.access.conglomerate.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.ReadObjectException;
import cn.edu.thu.tsfiledb.auth2.exception.WriteObjectException;

public class SerializeUtils {
	private static Logger logger = LoggerFactory.getLogger(SerializeUtils.class);

	/**
	 * read a string from stream. the length read should be exactly the same as
	 * MAX_LENGTH
	 * 
	 * @param raf
	 * @param maxLength
	 * @return
	 * @throws IOException
	 */
	public static String readString(RandomAccessFile raf, int maxLength) throws IOException {
		byte[] buffer = new byte[maxLength];
		int readCnt = raf.read(buffer, 0, maxLength);
		if (readCnt < maxLength) {
			logger.error("cannot read complete string {} < {}", readCnt, maxLength);
			throw new ReadObjectException();
		}
		int lastChar = 0;
		for (; lastChar < readCnt; lastChar++) {
			if (buffer[lastChar] == 0) {
				break;
			}
		}
		return new String(buffer, 0, lastChar);
	}

	/**
	 * convert a string to bytes and write to stream. the length of bytes should be
	 * no more than maxLength
	 * 
	 * @param raf
	 * @param source
	 * @param maxLength
	 * @throws IOException 
	 */
	public static void writeString(RandomAccessFile raf, String source, int maxLength)
			throws IOException {
		byte[] buffer = new byte[maxLength];
		byte[] nameBytes = source.getBytes("utf-8");
		if (nameBytes.length >= maxLength) {
			logger.error("String {} is too long", source);
			throw new WriteObjectException();
		}
		System.arraycopy(nameBytes, 0, buffer, 0, nameBytes.length);
		raf.write(buffer);
	}
}
