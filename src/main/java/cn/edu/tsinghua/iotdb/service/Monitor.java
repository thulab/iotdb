package cn.edu.tsinghua.iotdb.service;

import java.io.File;

import cn.edu.tsinghua.iotdb.conf.TsFileConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import org.apache.commons.io.FileUtils;

public class Monitor implements MonitorMBean, IService{
	private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	public static final Monitor INSTANCE = new Monitor();
    private final String MBEAN_NAME = String.format("%s:%s=%s", TsFileConstant.IOTDB_PACKAGE, TsFileConstant.JMX_TYPE, getID().getJmxName());

	@Override
	public long getDataSizeInByte() {
		try {
			return FileUtils.sizeOfDirectory(new File(config.dataDir));
		} catch (Exception e) {			
			return -1;
		}
	}

	@Override
	public int getFileNodeNum() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getOverflowCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getBufferWriteCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getBaseDirectory() {
		try {
			File file = new File(config.dataDir);
			return file.getAbsolutePath();
		} catch (Exception e) {
			return "Unavailable";
		}
	}

	@Override
	public boolean getWriteAheadLogStatus() {
		return config.enableWal;
	}

	@Override
	public long getMergePeriodInSecond() {
		return config.periodTimeForMerge;
	}

	@Override
	public long getClosePeriodInSecond() {
		return config.periodTimeForFlush;
	}

	@Override
	public void start() {
		JMXService.registerMBean(INSTANCE, MBEAN_NAME);
	}

	@Override
	public void stop() {
		JMXService.deregisterMBean(MBEAN_NAME);
	}

	@Override
	public ServiceType getID() {
		return ServiceType.MONITOR_SERVICE;
	}

}
