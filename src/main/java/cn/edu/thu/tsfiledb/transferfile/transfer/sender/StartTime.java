package cn.edu.thu.tsfiledb.transferfile.transfer.sender;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lylw on 2017/7/28.
 */
public class StartTime implements Serializable {
	private static final long serialVersionUID = 5485945375774889393L;
	private String nameSpace;
	private Map<String,Long> startTime = new HashMap<>();

	public StartTime(String nameSpace, Map<String,Long> startTime) {
		this.nameSpace = nameSpace;
		this.startTime.putAll(startTime);
	}

	public String getNameSpace() {
		return nameSpace;
	}

	public void setNameSpace(String nameSpace) {
		this.nameSpace = nameSpace;
	}

	public Map<String, Long> getStartTime() {
		return startTime;
	}

	public void setStartTime(Map<String,Long> startTime) {
		this.startTime.putAll(startTime);
	}
}
