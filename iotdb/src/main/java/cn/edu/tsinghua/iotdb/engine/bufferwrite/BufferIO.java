package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.write.writer.TsFileOutput;

public class BufferIO extends TsFileIOWriter {

	private int lastRowGroupIndex = 0;
	/**
	 * chunk group metadata which are not serialized on disk.
	 */
	private List<ChunkGroupMetaData> append;

	BufferIO(TsFileOutput tsFileOutput, List<ChunkGroupMetaData> rowGroups)
			throws IOException {
		super(tsFileOutput,rowGroups);
		lastRowGroupIndex = rowGroups.size();
		append = new ArrayList<>();
	}

	/**
	 * get all the chunkGroups' metadata which are appended after the last calling of this method,
	 * or after the class instance is initialized if this is the first time to call the method.
	 * @return a list of chunkgroup metadata
	 */
	List<ChunkGroupMetaData> getAppendedRowGroupMetadata() {
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


}
