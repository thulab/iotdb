package cn.edu.tsinghua.iotdb.engine.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;

/**
 * This class is used to cache <code>RowGroupBlockMetaDataCache</code> of tsfile
 * in IoTDB.
 * 
 * @author liukun
 *
 */
public class RowGroupBlockMetaDataCache {

	/*
	 * the default LRU cache size is 100
	 */
	private static RowGroupBlockMetaDataCache instance = new RowGroupBlockMetaDataCache(100);;
	private LinkedHashMap<String, TsRowGroupBlockMetaData> cache;

	/**
	 * This class is a map used to cache the <code>RowGroupBlockMetaData</code>.
	 * The caching strategy is LRU.
	 * 
	 * @author liukun
	 *
	 */
	private class LRULinkedHashMap extends LinkedHashMap<String, TsRowGroupBlockMetaData> {

		private static final long serialVersionUID = 1290160928914532649L;
		private int maxCapacity;

		public LRULinkedHashMap(int maxCapacity, boolean isLRU) {
			super(maxCapacity, 0.75f, isLRU);
			this.maxCapacity = maxCapacity;
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, TsRowGroupBlockMetaData> eldest) {
			return size() > maxCapacity;
		}
	}

	private RowGroupBlockMetaDataCache(int cacheSize) {
		cache = new LRULinkedHashMap(cacheSize, true);
	}

	public static RowGroupBlockMetaDataCache getInstance() {
		return instance;
	}

	public TsRowGroupBlockMetaData get(String filePath, String deltaObjectId, TsFileMetaData fileMetaData) {

		String jointPath = filePath + deltaObjectId;
		jointPath = jointPath.intern();
		synchronized (cache) {
			if (cache.containsKey(jointPath)) {
				return cache.get(jointPath);
			}
		}
		synchronized (jointPath) {
			synchronized (cache) {
				if (cache.containsKey(jointPath)) {
					return cache.get(jointPath);
				}
			}
			TsRowGroupBlockMetaData blockMetaData = TsFileMetadataUtils.getTsRowGroupBlockMetaData(filePath,
					deltaObjectId, fileMetaData);
			synchronized (cache) {
				cache.put(jointPath, blockMetaData);
				return cache.get(jointPath);
			}
		}
	}

	public void clear() {
		synchronized (cache) {
			cache.clear();
		}
	}
}
