package cn.edu.thu.tsfiledb.auth2.model;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.auth2.manage.AuthConfig;

public class Rolemeta {
	private static String roleFolder = AuthConfig.roleFolder;
	private static String metaPath = "roleInfo.meta";
	private static Integer metaMutex = new Integer(0);

	private static Rolemeta instance;

	private Rolemeta() {

	}

	public static Rolemeta getInstance() throws IOException {
		if (instance == null) {
			instance = new Rolemeta();
			instance.init();
		}
		return instance;
	}

	private void init() throws IOException {
		File roleFolderFile = new File(getRoleFolder());
		roleFolderFile.mkdirs();
		File metaFile = new File(roleFolder + metaPath);
		if (!metaFile.exists() || metaFile.length() < Integer.BYTES) {
			RandomAccessFile raf = new RandomAccessFile(metaFile, "rw");
			raf.writeInt(0);
			raf.close();
		}

	}

	public int getMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(roleFolder + metaPath, "rw");
			return raf.readInt();
		}
	}

	/**
	 * add 1 to MaxRID and write it back to the file
	 * 
	 * @throws IOException
	 */
	public void increaseMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(roleFolder + metaPath, "rw");
			int maxRID = raf.readInt();
			raf.seek(0);
			raf.writeInt(maxRID + 1);
		}
	}

	public String getMetaPath() {
		return metaPath;
	}

	public void setMetaPath(String metaPath) {
		Rolemeta.metaPath = metaPath;
	}

	public static String getRoleFolder() {
		return roleFolder;
	}

	public static void setRoleFolder(String roleFolder) {
		Rolemeta.roleFolder = roleFolder;
	}
}
