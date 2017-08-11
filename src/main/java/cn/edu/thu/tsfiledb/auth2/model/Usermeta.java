package cn.edu.thu.tsfiledb.auth2.model;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.conf.AuthConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class Usermeta {
	private static AuthConfig authConfig = TsfileDBDescriptor.getInstance().getConfig().authConfig;
	
	private static Integer metaMutex = new Integer(0);
	private static String metaPath = authConfig.USER_META_FILE;
	private static String userFolder = authConfig.USER_FOLDER;

	private static Usermeta instance;

	private Usermeta() {

	}

	public static Usermeta getInstance() throws IOException {
		if (instance == null) {
			instance = new Usermeta();
			instance.init();
		}
		return instance;
	}

	private void init() throws IOException {
		File roleFolder = new File(getUserFolder());
		roleFolder.mkdirs();
		File metaFile = new File(userFolder + metaPath);
		if (!metaFile.exists() || metaFile.length() < Integer.BYTES) {
			RandomAccessFile raf = new RandomAccessFile(metaFile, "rw");
			raf.writeInt(0);
			raf.close();
		}

	}

	public int getMaxUID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(userFolder + metaPath, "rw");
			return raf.readInt();
		}
	}

	/**
	 * add 1 to MaxUID and write it back to the file
	 * 
	 * @throws IOException
	 */
	public void increaseMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(userFolder + metaPath, "rw");
			int maxRID = raf.readInt();
			raf.seek(0);
			raf.writeInt(maxRID + 1);
		}
	}

	public static String getMetaPath() {
		return metaPath;
	}

	public static void setMetaPath(String metaPath) {
		Usermeta.metaPath = metaPath;
	}

	public static String getUserFolder() {
		return userFolder;
	}

	public static void setUserFolder(String userFolder) {
		Usermeta.userFolder = userFolder;
	}
}
