package cn.edu.thu.tsfiledb.engine.filenode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This is used to store information about filenodeProcessor status.<br>
 * lastUpdateTime will changed and stored by bufferwrite flush or bufferwrite close<br>
 * emptyIntervalFileNode and newFileNodes will changed and stored by overflow flush and overflow close<br>
 * fileNodeProcessorState will changed and stored by work->merge merge->wait wait->work<br>
 * numOfMergeFile will changed and stored in work to merge
 * 
 * @author liukun
 *
 */
public class FileNodeProcessorStore implements Serializable {

	private static final long serialVersionUID = -54525372941897565L;

	private  Map<String,Long> lastUpdateTimeMap;
	private  long lastUpdateTime;
	private  IntervalFileNode emptyIntervalFileNode;
	private  List<IntervalFileNode> newFileNodes;
	private  int numOfMergeFile;
	private  FileNodeProcessorStatus fileNodeProcessorStatus;

	public FileNodeProcessorStore(long lastUpdateTime, IntervalFileNode emptyIntervalFileNode,
			List<IntervalFileNode> newFileNodes, FileNodeProcessorStatus fileNodeProcessorStatus,int numOfMergeFile) {
		this.lastUpdateTime = lastUpdateTime;
		this.emptyIntervalFileNode = emptyIntervalFileNode;
		this.newFileNodes = newFileNodes;
		this.fileNodeProcessorStatus = fileNodeProcessorStatus;
		this.numOfMergeFile = numOfMergeFile;
	}
	
	public FileNodeProcessorStore(long lastUpdateTime, Map<String,Long> lastUpdateTimeMap,IntervalFileNode emptyIntervalFileNode,
			List<IntervalFileNode> newFileNodes, FileNodeProcessorStatus fileNodeProcessorStatus,int numOfMergeFile) {
		this.lastUpdateTime = lastUpdateTime;
		this.lastUpdateTimeMap = lastUpdateTimeMap;
		this.emptyIntervalFileNode = emptyIntervalFileNode;
		this.newFileNodes = newFileNodes;
		this.fileNodeProcessorStatus = fileNodeProcessorStatus;
		this.numOfMergeFile = numOfMergeFile;
	}
	
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}
	
	public Map<String,Long> getLastUpdateTimeMap(){
		return lastUpdateTimeMap;
	}
	
	public void setLastUpdateTimeMap(Map<String,Long> lastUpdateTimeMap){
		this.lastUpdateTimeMap = lastUpdateTimeMap;
	}
	
	public IntervalFileNode getEmptyIntervalFileNode() {
		return emptyIntervalFileNode;
	}
	public List<IntervalFileNode> getNewFileNodes() {
		return newFileNodes;
	}
	public FileNodeProcessorStatus getFileNodeProcessorState() {
		return fileNodeProcessorStatus;
	}

	public int getNumOfMergeFile() {
		return numOfMergeFile;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

	public void setEmptyIntervalFileNode(IntervalFileNode emptyIntervalFileNode) {
		this.emptyIntervalFileNode = emptyIntervalFileNode;
	}

	public void setNewFileNodes(List<IntervalFileNode> newFileNodes) {
		this.newFileNodes = newFileNodes;
	}

	public void setNumOfMergeFile(int numOfMergeFile) {
		this.numOfMergeFile = numOfMergeFile;
	}

	public void setFileNodeProcessorState(FileNodeProcessorStatus fileNodeProcessorState) {
		this.fileNodeProcessorStatus = fileNodeProcessorState;
	}
}
