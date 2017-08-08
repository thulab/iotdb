package cn.edu.thu.tsfiledb.auth2.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Rolemeata {
	private static String RoleFolder = "role/"; 
	private static String metaPath = "roleInfo.meta";
	private static Integer metaMutex = new Integer(0);
	
	private static Rolemeata instance;
	
	private Rolemeata() {
		
	}
	
	public static Rolemeata getInstance() throws IOException {
		if(instance == null) {
			instance = new Rolemeata();
			instance.init();
		}
		return instance;
	}
	
	private void init() throws IOException {
		File roleFolder = new File(getRoleFolder());
		roleFolder.mkdirs();
		File metaFile = new File(roleFolder + metaPath);{
			if(!metaFile.exists() || metaFile.length() < Integer.BYTES) {
				RandomAccessFile raf = new RandomAccessFile(metaFile, "rw");
				raf.writeInt(0);
				raf.close();
			}
		}
	}
	
	public int getMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(RoleFolder + metaPath, "rw");
			return raf.readInt();
		}
	}
	
	/** add 1 to MaxRID and write it back to the file
	 * @return the MaxRID before increment
	 * @throws IOException
	 */
	public int increaseMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(RoleFolder + metaPath, "rw");
			int maxRID = raf.readInt();
			raf.seek(0);
			raf.writeInt(maxRID + 1);
			return maxRID;
		}
	}

	public String getMetaPath() {
		return metaPath;
	}

	public void setMetaPath(String metaPath) {
		Rolemeata.metaPath = metaPath;
	}

	public static String getRoleFolder() {
		return RoleFolder;
	}

	public static void setRoleFolder(String roleFolder) {
		RoleFolder = roleFolder;
	}
}
