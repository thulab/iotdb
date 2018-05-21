package cn.edu.tsinghua.iotdb.postback.sender;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;

public class Utils {
	private static String[] snapshotPaths = PostBackSenderDescriptor.getInstance().getConfig().snapshotPaths;
	
	/**
	 * 
	 * @param filePath
	 * @return
	 */
	public static String getSnapshotFilePath(String filePath) {
		String[] name;
		String relativeFilePath;
		String os = System.getProperty("os.name");
		if (os.toLowerCase().startsWith("windows")) {
			name = filePath.split(File.separator + File.separator);
			relativeFilePath = "data" + File.separator + name[name.length-2] + File.separator + name[name.length-1];
		} else {
			name = filePath.split(File.separator);
			relativeFilePath = "data" + File.separator + name[name.length-2] + File.separator + name[name.length-1];
		}
		String bufferWritePath = name[0];
		for(int i = 1; i < name.length-2; i ++)
			bufferWritePath = bufferWritePath + File.separator + name[i];
		for(String snapshotPath: snapshotPaths) {
			if(snapshotPath.startsWith(bufferWritePath)) {
				if (!new File(snapshotPath).exists())
					new File(snapshotPath).mkdir();
				return snapshotPath + relativeFilePath;
			}
		}
		return null;
	}
	
	/**
	 * Verify sending list is empty or not
	 * 
	 * @param sendingFileList
	 * @return
	 */
	public static boolean isEmpty(Map<String, Set<String>> sendingFileList) {
		for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
			if (entry.getValue().size() != 0)
				return false;
		}
		return true;
	}
	
	public static void deleteFile(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} else {
			File[] files = file.listFiles();
			for (File f : files) {
				deleteFile(f);
				f.delete();
			}
		}
	}
}