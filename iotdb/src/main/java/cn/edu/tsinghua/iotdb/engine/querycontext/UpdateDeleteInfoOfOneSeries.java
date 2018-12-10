package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReaderImpl;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class UpdateDeleteInfoOfOneSeries {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDeleteInfoOfOneSeries.class);
    private TSDataType dataType;
    private List<OverflowUpdateDeleteFile> overflowUpdateFileList;
    private DynamicOneColumnData overflowUpdateInMem;
    private OverflowOperationReader overflowUpdateOperationReader;

    public OverflowOperationReader getOverflowUpdateOperationReader() {
        if (overflowUpdateOperationReader == null) {
            overflowUpdateOperationReader = new OverflowOperationReaderImpl(overflowUpdateInMem, overflowUpdateFileList, dataType);
        }
        return overflowUpdateOperationReader;
    }

	public OverflowOperationReader getOverflowUpdateOperationReaderNewInstance() {
		return new OverflowOperationReaderImpl(overflowUpdateInMem, overflowUpdateFileList, dataType);
	}

    public void setDataType(TSDataType dataType) {
		this.dataType = dataType;
	}

	public void setOverflowUpdateFileList(List<OverflowUpdateDeleteFile> overflowUpdateFileList) {
		this.overflowUpdateFileList = overflowUpdateFileList;
	}

	public void setOverflowUpdateInMem(DynamicOneColumnData overflowUpdateInMem) {
		this.overflowUpdateInMem = overflowUpdateInMem;
	}
	
	public TSDataType getDataType() {
		return dataType;
	}

	public List<OverflowUpdateDeleteFile> getOverflowUpdateFileList() {
		return overflowUpdateFileList;
	}

	public DynamicOneColumnData getOverflowUpdateInMem() {
		return overflowUpdateInMem;
	}
}
