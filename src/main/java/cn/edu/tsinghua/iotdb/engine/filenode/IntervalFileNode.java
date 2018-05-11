package cn.edu.tsinghua.iotdb.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.tombstone.LocalTombstoneFile;
import cn.edu.tsinghua.iotdb.engine.tombstone.TombstoneFile;
import cn.edu.tsinghua.iotdb.engine.tombstone.TombstoneFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to store one bufferwrite file status.<br>
 * 
 * @author liukun
 * @author kangrong
 *
 */
public class IntervalFileNode implements Serializable {

	private static final long serialVersionUID = -4309683416067212549L;
	private static final Logger logger = LoggerFactory.getLogger(IntervalFileNode.class);
	private String relativePath;
	public OverflowChangeType overflowChangeType;

	private Map<String, Long> startTimeMap;
	private Map<String, Long> endTimeMap;
	private Set<String> mergeChanged = new HashSet<>();
	private static String baseDir = TsfileDBDescriptor.getInstance().getConfig().bufferWriteDir;

	/**
	 * The tombstone file that is associated to this TsFile.
	 */
	transient private TombstoneFile tombstoneFile;

	public IntervalFileNode(Map<String, Long> startTimeMap, Map<String, Long> endTimeMap, OverflowChangeType type,
			String relativePath) {

		this.overflowChangeType = type;
		this.relativePath = relativePath;

		this.startTimeMap = startTimeMap;
		this.endTimeMap = endTimeMap;
	}

	/**
	 * This is just used to construct a new bufferwritefile
	 * 
	 * @param type
	 * @param relativePath
	 */
	public IntervalFileNode(OverflowChangeType type, String relativePath) {

		this.overflowChangeType = type;
		this.relativePath = relativePath;

		startTimeMap = new HashMap<>();
		endTimeMap = new HashMap<>();
	}

	public void setStartTime(String deltaObjectId, long startTime) {

		startTimeMap.put(deltaObjectId, startTime);
	}

	public long getStartTime(String deltaObjectId) {

		if (startTimeMap.containsKey(deltaObjectId)) {
			return startTimeMap.get(deltaObjectId);
		} else {
			return -1;
		}
	}

	public Map<String, Long> getStartTimeMap() {

		return startTimeMap;
	}

	public void setStartTimeMap(Map<String, Long> startTimeMap) {

		this.startTimeMap = startTimeMap;
	}

	public void setEndTimeMap(Map<String, Long> endTimeMap) {

		this.endTimeMap = endTimeMap;
	}

	public void setEndTime(String deltaObjectId, long timestamp) {

		this.endTimeMap.put(deltaObjectId, timestamp);
	}

	public long getEndTime(String deltaObjectId) {

		if (endTimeMap.get(deltaObjectId) == null) {
			return -1;
		}
		return endTimeMap.get(deltaObjectId);
	}

	public Map<String, Long> getEndTimeMap() {

		return endTimeMap;
	}

	public void removeTime(String deltaObjectId) {

		startTimeMap.remove(deltaObjectId);
		endTimeMap.remove(deltaObjectId);
	}

	public String getFilePath() {

		if (relativePath == null) {
			return relativePath;
		}
		return new File(baseDir, relativePath).getPath();
	}

	public void setRelativePath(String relativePath) {

		this.relativePath = relativePath;
	}

	public String getRelativePath() {

		return relativePath;
	}

	public boolean checkEmpty() {

		return startTimeMap.isEmpty() && endTimeMap.isEmpty();
	}

	public void clear() {

		startTimeMap.clear();
		endTimeMap.clear();
		mergeChanged.clear();
		overflowChangeType = OverflowChangeType.NO_CHANGE;
		relativePath = null;
		if (tombstoneFile != null) {
			try {
				tombstoneFile.close();
			} catch (IOException e) {
				logger.error("Cannot close tombstone file for {}", relativePath);
			}
		}
	}

	public void changeTypeToChanged(FileNodeProcessorStatus fileNodeProcessorState) {

		if (fileNodeProcessorState == FileNodeProcessorStatus.MERGING_WRITE) {
			overflowChangeType = OverflowChangeType.MERGING_CHANGE;
		} else {
			overflowChangeType = OverflowChangeType.CHANGED;
		}
	}

	public void addMergeChanged(String deltaObjectId) {

		mergeChanged.add(deltaObjectId);
	}

	public Set<String> getMergeChanged() {

		return mergeChanged;
	}

	public void clearMergeChanged() {

		mergeChanged.clear();
	}

	public boolean isClosed() {

		return !endTimeMap.isEmpty();

	}

	public IntervalFileNode backUp() {

		Map<String, Long> startTimeMap = new HashMap<>(this.startTimeMap);
		Map<String, Long> endTimeMap = new HashMap<>(this.endTimeMap);
		return new IntervalFileNode(startTimeMap, endTimeMap, overflowChangeType, relativePath);
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;
		result = prime * result + ((endTimeMap == null) ? 0 : endTimeMap.hashCode());
		result = prime * result + ((relativePath == null) ? 0 : relativePath.hashCode());
		result = prime * result + ((overflowChangeType == null) ? 0 : overflowChangeType.hashCode());
		result = prime * result + ((startTimeMap == null) ? 0 : startTimeMap.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntervalFileNode other = (IntervalFileNode) obj;
		if (endTimeMap == null) {
			if (other.endTimeMap != null)
				return false;
		} else if (!endTimeMap.equals(other.endTimeMap))
			return false;
		if (relativePath == null) {
			if (other.relativePath != null)
				return false;
		} else if (!relativePath.equals(other.relativePath))
			return false;
		if (overflowChangeType != other.overflowChangeType)
			return false;
		if (startTimeMap == null) {
			if (other.startTimeMap != null)
				return false;
		} else if (!startTimeMap.equals(other.startTimeMap))
			return false;
		return true;
	}

	@Override
	public String toString() {

		return String.format(
				"IntervalFileNode [relativePath=%s,overflowChangeType=%s, startTimeMap=%s, endTimeMap=%s, mergeChanged=%s]",
				relativePath, overflowChangeType, startTimeMap, endTimeMap, mergeChanged);
	}

	public TombstoneFile getTombstoneFile() throws IOException {
		if (tombstoneFile == null) {
			tombstoneFile = TombstoneFileFactory.getFactory().getTombstoneFile(baseDir + File.separator + this.relativePath + TombstoneFile.TOMBSTONE_SUFFIX);
		}
		return tombstoneFile;
	}

	public void closeTombstoneFile() throws IOException {
		if(tombstoneFile != null) {
			tombstoneFile.close();
		}
	}

	public Collection<String> listDeltaObjects() {
		return startTimeMap.keySet();
	}
}
