package cn.edu.tsinghua.iotdb.utils;

import java.io.File;
import java.io.IOException;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;

/**
 * <p>
 * This class is used for cleaning test environment in unit test and integration
 * test
 * </p>
 * 
 * @author liukun
 *
 */
public class EnvironmentUtils {

	private static TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

	public static void cleanEnv() throws IOException {
		// clean filenode manager
		try {
			if (!FileNodeManager.getInstance().closeAll()) {
				throw new IOException("Can't close the filenode manager");
			}
		} catch (FileNodeManagerException e) {
			throw new IOException(e.getMessage());
		}
		// clean wal
		WriteLogManager.getInstance().close();
		// close metadata
		MManager.getInstance().clear();
		MManager.getInstance().flushObjectToFile();
		// delete all directory
		cleanAllDir();
	}

	private static void cleanAllDir() throws IOException {
		// delete bufferwrite
		cleanDir(config.bufferWriteDir);
		// delete overflow
		cleanDir(config.overflowDataDir);
		// delete filenode
		cleanDir(config.fileNodeDir);
		// delete metadata
		cleanDir(config.metadataDir);
		// delete wal
		cleanDir(config.walFolder);
	}

	public static void cleanDir(String dir) throws IOException {
		File file = new File(dir);
		if (file.exists()) {
			if (file.isDirectory()) {
				for (File subFile : file.listFiles()) {
					cleanDir(subFile.getAbsolutePath());
				}
			}
			if (!file.delete()) {
				throw new IOException(String.format("The file %s can't be deleted", dir));
			}
		}
	}
}
