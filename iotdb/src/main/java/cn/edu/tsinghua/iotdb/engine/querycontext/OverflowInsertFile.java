package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.util.List;


public class OverflowInsertFile {

    // seriesChunkMetadata of selected series
    private List<ChunkMetaData> timeSeriesChunkMetaDatas;

    public OverflowInsertFile() {
    }

    public OverflowInsertFile(String path, List<ChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }

    public List<ChunkMetaData> getTimeSeriesChunkMetaDatas() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<ChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }
}
