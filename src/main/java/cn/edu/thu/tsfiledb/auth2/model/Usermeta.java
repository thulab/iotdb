package cn.edu.thu.tsfiledb.auth2.model;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Usermeta {
	private static Integer metaMutex;
	private static String metaPath = "userInfo.meta";
	
	public static int getMaxUID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(metaPath, "r");
			return raf.readInt();
		}
	}
	
	/** add 1 to MaxUID and write it back to the file
	 * @return the MaxUID before increment
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
		Usermeta.metaPath = metaPath;
	}
}
