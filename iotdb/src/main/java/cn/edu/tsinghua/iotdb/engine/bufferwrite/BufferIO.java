package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

public class BufferIO extends TsFileIOWriter {

	private int lastRowGroupIndex = 0;
	private List<ChunkGroupMetaData> append;

	public BufferIO(File output, long offset, List<ChunkGroupMetaData> rowGroups)
			throws IOException {
		super(output, offset, rowGroups);
		lastRowGroupIndex = rowGroups.size();
		append = new ArrayList<>();
	}

	public List<ChunkGroupMetaData> getAppendedRowGroupMetadata() {
		if (lastRowGroupIndex < getChunkGroupMetaDatas().size()) {
			append.clear();
			List<ChunkGroupMetaData> all = getChunkGroupMetaDatas();
			for (int i = lastRowGroupIndex; i < all.size(); i++) {
				append.add(all.get(i));
			}
			lastRowGroupIndex = all.size();
		}
		return append;
	}

	public long getPos() throws IOException {
		return super.getPos();
	}
}
