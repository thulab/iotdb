package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.util.List;


public class OverflowUpdateDeleteFile {
    private String filePath;
    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

    public OverflowUpdateDeleteFile(String filePath, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        this.filePath = filePath;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
    }

    public String getFilePath() {
        return filePath;
    }

    public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
        return timeSeriesChunkMetaDataList;
    }
}
