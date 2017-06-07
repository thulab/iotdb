package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to store one bufferwrite file status.<br>
 * 
 * @author liukun
 * @author kangrong
 *
 */
public class IntervalFileNode implements Serializable {
	private static final long serialVersionUID = -4309683416067212549L;
//	public long startTime;
//	public long endTime;
	public OverflowChangeType overflowChangeType;
	public String filePath;

	private Map<String,Long> startTimeMap;
	private Map<String,Long> endTimeMap; 
	
	

	public IntervalFileNode(Map<String, Long> startTimeMap, Map<String, Long> endTimeMap, OverflowChangeType type,
			String filePath) {
		
		this.overflowChangeType = type;
		this.filePath = filePath;

		this.startTimeMap = startTimeMap;
		this.endTimeMap = endTimeMap;
	}

	/**
	 * This is just used to construct a new bufferwritefile
	 * 
	 * @param startTime
	 * @param type
	 * @param filePath
	 * @param errFilePath
	 */
	public IntervalFileNode(OverflowChangeType type, String filePath) {
		
		this.overflowChangeType = type;
		this.filePath = filePath;

		startTimeMap = new HashMap<>();
		endTimeMap = new HashMap<>();
	}
	
	public void setStartTime(String deltaObjectId, long startTime){
		
		startTimeMap.put(deltaObjectId, startTime);
	}
	
	public long getStartTime(String deltaObjectId){
		
		if(startTimeMap.containsKey(deltaObjectId)){
			return startTimeMap.get(deltaObjectId);
		}else{
			return -1;
		}
	}
	
	public Map<String,Long> getStartTimeMap(){
		return startTimeMap;
	}
	
	public void setEndTimeMap(Map<String,Long> endTimeMap){
		
		this.endTimeMap = endTimeMap;
	}
	
	public void setStartTimeMap(Map<String,Long> startTimeMap){
		
		this.startTimeMap = startTimeMap;
	}
	
	public void setEndTime(String deltaObjectId, long timestamp){
		
		this.endTimeMap.put(deltaObjectId, timestamp);
	}
	
	public long getEndTime(String deltaObjectId){
		
		return endTimeMap.get(deltaObjectId);
	}
	
	public Map<String,Long> getEndTimeMap(){
		
		return endTimeMap;
	}

	public void changeTypeToChanged(FileNodeProcessorStatus fileNodeProcessorState) {

		if (fileNodeProcessorState == FileNodeProcessorStatus.MERGING_WRITE) {
			overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		} else {
			overflowChangeType = OverflowChangeType.CHANGED;
		}
	}

	public boolean changeTypeToUnChanged() {
		switch (overflowChangeType) {
		case NO_CHANGE:
			return false;
		case CHANGED:
			overflowChangeType = OverflowChangeType.NO_CHANGE;
			return true;
		case MERGING_CHANGE:
			overflowChangeType = OverflowChangeType.CHANGED;
			return true;
		default:
			throw new UnsupportedOperationException(overflowChangeType.toString());
		}
	}

	public boolean isClosed() {
		
		return !endTimeMap.isEmpty();

	}

	public IntervalFileNode backUp() {
		return new IntervalFileNode(startTimeMap,endTimeMap,overflowChangeType, filePath);
	}
}
