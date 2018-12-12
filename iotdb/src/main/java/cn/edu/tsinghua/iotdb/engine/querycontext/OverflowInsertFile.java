package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.util.List;


public class OverflowInsertFile {

    private String filePath;

    // seriesChunkMetadata of selected series
    private List<ChunkMetaData> timeSeriesChunkMetaDatas;

    public OverflowInsertFile() {
    }

    public OverflowInsertFile(String path, List<ChunkMetaData> timeSeriesChunkMetaDatas) {
        this.filePath = path;
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }

    public String getFilePath() {
        return filePath;
    }

    public List<ChunkMetaData> getChunkMetaDataList() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<ChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }
}
