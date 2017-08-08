package cn.edu.thu.tsfiledb.service;

public interface MonitorMBean {
	long getDataSizeInByte();
	double getCpuRatio();
	int getFileNodeNum();
	long getOverflowCacheSize();
	long getBufferWriteCacheSize();
	long getMergePeriodInSecond();
	long getClosePeriodInSecond();
	
	String getBaseDirectory();
	boolean getWriteAheadLogStatus();
}
