package cn.edu.tsinghua.iotdb.engine.cache;

import java.util.concurrent.ConcurrentHashMap;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;

public class TsFileMetaDataCache {

	private static TsFileMetaDataCache instance;
	private ConcurrentHashMap<String, TsFileMetaData> cache;

	private TsFileMetaDataCache() {
		// TODO Auto-generated constructor stub
		cache = new ConcurrentHashMap<>();
	}

	public static TsFileMetaDataCache getInstance() {
		return instance;
	}

	public TsFileMetaData get(String path) {
		if (cache.contains(path)) {
			return cache.get(path);
		} else {
			// put value into cache
			return cache.get(path);
		}
	}

	public void remove(String path) {
		// TODO remove the FileMetaData with the special key of path
		cache.remove(path);
	}

	public void clear() {
		// TODO remove all the FileMetaData
		cache.clear();
	}

	private void add(String path) {
		// TODO add the special FileMetaData with the key of path
		cache.put(path, null);
	}

}
