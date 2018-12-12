package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.util.List;


public class OverflowInsertFile {

    private String filePath;

    // seriesChunkMetadata of selected series
    private List<ChunkMetaData> timeSeriesChunkMetaDatas;

    public OverflowInsertFile() {
    }

    public OverflowInsertFile(String path, List<ChunkMetaData> timeSeriesChunkMetaData) {
        this.filePath = path;
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaData;
    }

    public String getFilePath() {
        return filePath;
    }

    public List<ChunkMetaData> getChunkMetaDataList() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<ChunkMetaData> timeSeriesChunkMetaData) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaData;
    }
}
