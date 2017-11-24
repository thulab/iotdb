package cn.edu.tsinghua.iotdb.engine.cache;

import java.util.concurrent.ConcurrentHashMap;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;

/**
 * This class is used to cache <code>TsFileMetaData</code> of tsfile in IoTDB.
 * 
 * @author liukun
 *
 */
public class TsFileMetaDataCache {
	
	/** key: The file path of tsfile */
	private ConcurrentHashMap<String, TsFileMetaData> cache;

	private TsFileMetaDataCache() {
		cache = new ConcurrentHashMap<>();
	}

	/*
	 * Singleton pattern
	 */
	private static class TsFileMetaDataCacheHolder {
		private static final TsFileMetaDataCache INSTANCE = new TsFileMetaDataCache();
	}

	public static TsFileMetaDataCache getInstance() {
		return TsFileMetaDataCacheHolder.INSTANCE;
	}

	public TsFileMetaData get(String path) {

		path = path.intern();
		synchronized (path) {
			if (!cache.containsKey(path)) {
				// read value from tsfile
				TsFileMetaData fileMetaData = TsFileMetadataUtils.getTsFileMetaData(path);
				cache.put(path, fileMetaData);
			}
			return cache.get(path);
		}
	}

	public void remove(String path) {
		cache.remove(path);
	}

	public void clear() {
		cache.clear();
	}
}
