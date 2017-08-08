package cn.edu.thu.tsfiledb.auth2.model;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Rolemeata {
	private static String RoleFolder = "role/"; 
	private static String metaPath = "roleInfo.meta";
	private static Integer metaMutex = new Integer(0);
	
	private static Rolemeata instance;
	
	private Rolemeata() {
		
	}
	
	public static Rolemeata getInstance() {
		if(instance == null) {
			instance = new Rolemeata();
			instance.init();
		}
		return instance;
	}
	
	private void init() {
		File roleFolder = new File(RoleFolder);
		roleFolder.mkdirs();
	}
	
	public int getMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(metaPath, "r");
			return raf.readInt();
		}
	}
	
	/** add 1 to MaxRID and write it back to the file
	 * @return the MaxRID before increment
	 * @throws IOException
	 */
	public int increaseMaxRID() throws IOException {
		synchronized (metaMutex) {
			RandomAccessFile raf = new RandomAccessFile(metaPath, "rw");
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
}
