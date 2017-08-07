package cn.edu.thu.tsfiledb.auth2.model;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Rolemeata {
	private static String metaPath = "roleInfo.meta";
	private static Integer metaMutex = new Integer(0);
	
	public static int getMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(metaPath, "r");
			return raf.readInt();
		}
	}
	
	/** add 1 to MaxRID and write it back to the file
	 * @return the MaxRID before increment
	 * @throws IOException
	 */
	public static int increaseMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(metaPath, "rw");
			int maxRID = raf.readInt();
			raf.seek(0);
			raf.writeInt(maxRID + 1);
			return maxRID;
		}
	}

	public static String getMetaPath() {
		return metaPath;
	}

	public static void setMetaPath(String metaPath) {
		Rolemeata.metaPath = metaPath;
	}
}
