package cn.edu.tsinghua.iotdb.service;

import java.io.File;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.OpenFileNumUtil;
import org.apache.commons.io.FileUtils;

public class Monitor implements MonitorMBean{
	private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	
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
	public int getTotalOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(0);
	}

	@Override
	public int getDataOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(1);
	}

	@Override
	public int getDeltaOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(2);
	}

	@Override
	public int getOverflowOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(3);
	}

	@Override
	public int getWalOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(4);
	}

	@Override
	public int getMetadataOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(5);
	}

	@Override
	public int getDigestOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(6);
	}

	@Override
	public int getSocketOpenFileNum() {
		return OpenFileNumUtil.getInstance().get().get(7);
	}

	@Override
	public long getMergePeriodInSecond() {
		return config.periodTimeForMerge;
	}

	@Override
	public long getClosePeriodInSecond() {
		return config.periodTimeForFlush;
	}

}
