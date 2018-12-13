package cn.edu.tsinghua.iotdb.engine.filenode;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * This is a structure for a query result. The result of query contains four
 * parts. The first part is data in memory which contain the
 * {@code DynamicOneColumnData}currentPage and
 * {@code Pair<List<ByteArrayInputStream>, CompressionTypeName>} pageList. The
 * second part is data in the no closed file which contain
 * {@code List<RowGroupMetaData>} bufferwriteDataInDisk. The third part is data
 * in closed file which contain {@code List<IntervalFileNode>}
 * bufferwriteDataInFiles. The fourth part is data in overflow which contain the
 * {@code List<Object>} allOverflowData.
 * 
 * @author liukun
 *
 */
public class QueryStructure {

	private final DynamicOneColumnData currentPage;

	private final Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList;

	private final List<ChunkGroupMetaData> bufferwriteDataInDisk;

	private final List<IntervalFileNode> bufferwriteDataInFiles;

	private final List<Object> allOverflowData;

	public QueryStructure(DynamicOneColumnData currentPage,
			Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList,
			List<ChunkGroupMetaData> bufferwriteDataInDisk, List<IntervalFileNode> bufferwriteDataInFiles,
			List<Object> allOverflowData) {
		this.currentPage = currentPage;
		this.pageList = pageList;
		this.bufferwriteDataInDisk = bufferwriteDataInDisk;
		this.bufferwriteDataInFiles = bufferwriteDataInFiles;
		this.allOverflowData = allOverflowData;
	}

	public DynamicOneColumnData getCurrentPage() {
		return currentPage;
	}

	public Pair<List<ByteArrayInputStream>, CompressionTypeName> getPageList() {
		return pageList;
	}

	public List<ChunkGroupMetaData> getBufferwriteDataInDisk() {
		return bufferwriteDataInDisk;
	}

	public List<IntervalFileNode> getBufferwriteDataInFiles() {
		return bufferwriteDataInFiles;
	}

	public List<Object> getAllOverflowData() {
		return allOverflowData;
	}

	@Override
	public String toString() {
		return "QueryStructure [currentPage=" + currentPage + ", pageList=" + pageList + ", bufferwriteDataInDisk="
				+ bufferwriteDataInDisk + ", bufferwriteDataInFiles=" + bufferwriteDataInFiles + ", allOverflowData="
				+ allOverflowData + "]";
	}

}
