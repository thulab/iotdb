package cn.edu.tsinghua.iotdb.engine.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

public class RowGroupBlockMetaDataCache {
	/*
	 * the default LRU cache size is 100
	 */
	private static RowGroupBlockMetaDataCache instance = new RowGroupBlockMetaDataCache(100);;
	private LinkedHashMap<String, RowGroupBlockMetaData> cache;

	private class LRULinkedHashMap extends LinkedHashMap<String, RowGroupBlockMetaData> {

		private static final long serialVersionUID = 1290160928914532649L;
		private int maxCapacity;

		public LRULinkedHashMap(int maxCapacity, boolean isLRU) {
			super(maxCapacity, 0.75f, isLRU);
			this.maxCapacity = maxCapacity;
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, RowGroupBlockMetaData> eldest) {
			return size() > maxCapacity;
		}

	}

	private RowGroupBlockMetaDataCache(int cacheSize) {
		cache = new LRULinkedHashMap(cacheSize, true);
	}

	public static RowGroupBlockMetaDataCache getInstance() {
		return instance;
	}

	public RowGroupBlockMetaData get(String filePath, String deltaObject, TsFileMetaData fileMetaData) {
		if (cache.containsKey(filePath)) {
			return cache.get(filePath);
		} else {
			// add data int cache
			return cache.get(filePath);
		}
	}

	public void clear() {
		cache.clear();
	}
}
