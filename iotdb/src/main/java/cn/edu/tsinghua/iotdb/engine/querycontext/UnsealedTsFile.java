package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class UnsealedTsFile {
    private String filePath;
    private List<ChunkMetaData> timeSeriesChunkMetaDatas;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public List<ChunkMetaData> getTimeSeriesChunkMetaDatas() {
        return timeSeriesChunkMetaDatas;
    }

    public void setTimeSeriesChunkMetaDatas(List<ChunkMetaData> timeSeriesChunkMetaDatas) {
        this.timeSeriesChunkMetaDatas = timeSeriesChunkMetaDatas;
    }
}
