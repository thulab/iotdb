package cn.edu.thu.tsfiledb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.derby.iapi.store.access.conglomerate.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;
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
		return bytesToStr(buffer);
	}
	
	public static String readString(ByteBuffer buffer, int maxLength) {
		byte[] bytes = new byte[maxLength];
		buffer.get(bytes);
		return bytesToStr(bytes);
	}
	
	public static String bytesToStr(byte[] buffer) {
		int lastChar = 0;
		for (; lastChar < buffer.length; lastChar++) {
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
	public static void writeString(RandomAccessFile raf, String source, int maxLength) throws IOException {
		if (source == null) {
			raf.write(new byte[maxLength]);
		} else {
			byte[] buffer = new byte[maxLength];
			byte[] nameBytes = source.getBytes("utf-8");
			if (nameBytes.length >= maxLength) {
				logger.error("String {} is too long {} > {}", source, nameBytes.length, maxLength);
				throw new WriteObjectException();
			}
			System.arraycopy(nameBytes, 0, buffer, 0, nameBytes.length);
			raf.write(buffer);
		}
	}
	
	public static byte[] strToBytes(String source, int maxLength) throws IOException {
		byte[] buffer = new byte[maxLength];
		if (source == null) {
			return buffer;
		}
		byte[] nameBytes = source.getBytes("utf-8");
		if (nameBytes.length >= maxLength) {
			logger.error("String {} is too long {} > {}", source, nameBytes.length, maxLength);
			throw new WriteObjectException();
		}
		System.arraycopy(nameBytes, 0, buffer, 0, nameBytes.length);
		return buffer;
	}
	
	public static byte[] intToBytes(int num) {
		byte[] ret = new byte[Integer.BYTES];
		ret[0] = (byte) ((num >> 24) & 0xff);
		ret[1] = (byte) ((num >> 16) & 0xff);
		ret[2] = (byte) ((num >> 8) & 0xff);
		ret[3] = (byte) ((num >> 0) & 0xff);
		return ret;
	}
	
	public static byte[] intToBytes(int num, byte[] bytes, int offset) {
		bytes[offset + 0] = (byte) ((num >> 24) & 0xff);
		bytes[offset + 1] = (byte) ((num >> 16) & 0xff);
		bytes[offset + 2] = (byte) ((num >> 8) & 0xff);
		bytes[offset + 3] = (byte) ((num >> 0) & 0xff);
		return bytes;
	}
	
	public static int bytesToInt(byte[] bytes, int offset) {
		return (int) (bytes[offset + 0]) << 24 +
				(int) (bytes[offset + 1]) << 16 +
				(int) (bytes[offset + 2]) << 8 +
				(int) (bytes[offset + 3]) << 0;
	}
	
	public static byte[] intsToBytes(int[] ints) {
		byte[] ret = new byte[ints.length * Integer.BYTES];
		for(int i = 0; i < ints.length; i++) {
			ret[i * 4  + 0] = (byte) ((ints[i] >> 24) & 0xff);
			ret[i * 4  + 1] = (byte) ((ints[i] >> 16) & 0xff);
			ret[i * 4  + 2] = (byte) ((ints[i] >> 8) & 0xff);
			ret[i * 4  + 3] = (byte) ((ints[i] >> 0) & 0xff);
		}
		return ret;
	}
	
	public static int[] bytesToInts(byte[] bytes, int offset, int cnt) {
		int[] ret = new int[cnt];
		for(int i = 0; i < cnt; i++) {
			ret[i] = (int) (bytes[i * 4 + 0 + offset]) << 24 +
					(int) (bytes[i * 4 + 1 + offset]) << 16 +
					(int) (bytes[i * 4 + 2 + offset]) << 8 +
					(int) (bytes[i * 4 + 3 + offset]) << 0;
		}
		return ret;
	}
}
