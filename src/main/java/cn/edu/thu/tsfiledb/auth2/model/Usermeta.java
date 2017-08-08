package cn.edu.thu.tsfiledb.auth2.model;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Usermeta {
	private static Integer metaMutex;
	private static String metaPath = "userInfo.meta";
	private static String userFolder = "user/";

	private static Usermeta instance;

	private Usermeta() {

	}

	public static Usermeta getInstance() {
		if (instance == null) {
			instance = new Usermeta();
			instance.init();
		}
		return instance;
	}

	private void init() {
		File roleFolder = new File(userFolder);
		roleFolder.mkdirs();
	}

	public static int getMaxUID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(metaPath, "r");
			return raf.readInt();
		}
	}

	/**
	 * add 1 to MaxUID and write it back to the file
	 * 
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
