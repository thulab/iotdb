package cn.edu.tsinghua.iotdb.engine.bufferwriteV2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

public class BufferWriteIO extends TsFileIOWriter {

	private int lastRowGroupIndex = 0;

	public BufferWriteIO(ITsRandomAccessFileWriter output, long offset, List<RowGroupMetaData> rowGroups)
			throws IOException {
		super(output, offset, rowGroups);
		lastRowGroupIndex = rowGroups.size();
	}

	public List<RowGroupMetaData> getAppendedRowGroupMetadata() {
		List<RowGroupMetaData> append = new ArrayList<>();
		List<RowGroupMetaData> all = getRowGroups();
		for (int i = lastRowGroupIndex; i < all.size(); i++) {
			append.add(all.get(i));
		}
		return append;
	}
}
