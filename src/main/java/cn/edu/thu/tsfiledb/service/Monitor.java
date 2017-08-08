package cn.edu.thu.tsfiledb.service;

import java.io.File;
import java.lang.management.ManagementFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.commons.io.FileUtils;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class Monitor implements MonitorMBean {
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
	public long getMergePeriodInSecond() {
		return config.periodTimeForMerge;
	}

	@Override
	public long getClosePeriodInSecond() {
		return config.periodTimeForClose;
	}

	@Override
	public double getCpuRatio() {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name;
		try {
			name = ObjectName.getInstance("java.lang:type=OperatingSystem");
			AttributeList list = mbs.getAttributes(name, new String[] { "ProcessCpuLoad" });
			if (list.isEmpty())
				return Double.NaN;

			Attribute att = (Attribute) list.get(0);
			Double value = (Double) att.getValue();

			// usually takes a couple of seconds before we get real values
			if (value == -1.0)
				return Double.NaN;
			// returns a percentage value with 1 decimal point precision
			return ((int) (value * 1000) / 10.0);
		} catch (MalformedObjectNameException | NullPointerException | InstanceNotFoundException | ReflectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;

	}

}
