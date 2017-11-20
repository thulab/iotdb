package cn.edu.tsinghua.iotdb.engine.cache;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;

public class TsFileMetaDataCache {

	private static TsFileMetaDataCache instance;

	private TsFileMetaDataCache() {
		// TODO Auto-generated constructor stub
	}

	public static TsFileMetaDataCache getInstance() {
		return instance;
	}

	public TsFileMetaData get(String path) {
		return null;
	}

	public void remove(String path) {
		// TODO remove the FileMetaData with the special key of path
	}

	public void clear() {
		// TODO remove all the FileMetaData
	}

	private void add(String path) {
		// TODO add the special FileMetaData with the key of path
	}

}
