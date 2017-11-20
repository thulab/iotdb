package cn.edu.tsinghua.iotdb.engine.cache;

import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

public class RowGroupBlockMetaDataCache {

	private static RowGroupBlockMetaDataCache instance;
	private int cacheSize;

	private RowGroupBlockMetaDataCache() {

	}

	public static RowGroupBlockMetaDataCache getInstance() {
		return null;
	}

	public RowGroupBlockMetaData get() {
		return null;
	}
	
	public void clear(){
		
	}
}
