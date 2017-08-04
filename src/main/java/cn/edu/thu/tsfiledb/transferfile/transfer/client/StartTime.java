package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import java.io.Serializable;

/**
 * Created by lylw on 2017/7/28.
 */
public class StartTime implements Serializable {
	private static final long serialVersionUID = 5485945375774889393L;
	private String device;
	private long startTime;

	public StartTime(String device, long startTime) {
		this.device = device;
		this.startTime = startTime;
	}

	public String getDevice() {
		return device;
	}

	public void setDevice(String device) {
		this.device = device;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
}
