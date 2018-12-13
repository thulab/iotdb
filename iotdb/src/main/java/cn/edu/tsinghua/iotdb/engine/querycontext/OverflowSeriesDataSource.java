package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;

import java.util.List;


public class OverflowSeriesDataSource {

    private Path seriesPath;
    private TSDataType dataType;

    // overflow tsfile
    private List<OverflowInsertFile> overflowInsertFileList;

    // unSeq mem-table
    private RawSeriesChunk rawChunk;

    private UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries;

    public OverflowSeriesDataSource(Path seriesPath) {
        this.seriesPath = seriesPath;
    }

    public OverflowSeriesDataSource(Path seriesPath, TSDataType dataType, List<OverflowInsertFile> overflowInsertFileList, RawSeriesChunk rawChunk, UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries) {
        this.seriesPath = seriesPath;
        this.dataType = dataType;
        this.overflowInsertFileList = overflowInsertFileList;
        this.rawChunk = rawChunk;
        this.updateDeleteInfoOfOneSeries = updateDeleteInfoOfOneSeries;
    }

    public List<OverflowInsertFile> getOverflowInsertFileList() {
        return overflowInsertFileList;
    }

    public void setOverflowInsertFileList(List<OverflowInsertFile> overflowInsertFileList) {
        this.overflowInsertFileList = overflowInsertFileList;
    }

    public UpdateDeleteInfoOfOneSeries getUpdateDeleteInfoOfOneSeries() {
        return updateDeleteInfoOfOneSeries;
    }

    public void setUpdateDeleteInfoOfOneSeries(UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries) {
        this.updateDeleteInfoOfOneSeries = updateDeleteInfoOfOneSeries;
    }

    public RawSeriesChunk getRawChunk() {
        return rawChunk;
    }

    public void setRawChunk(RawSeriesChunk rawChunk) {
        this.rawChunk = rawChunk;
    }

    public Path getSeriesPath() {
        return seriesPath;
    }

    public void setSeriesPath(Path seriesPath) {
        this.seriesPath = seriesPath;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public boolean hasRawChunk() {
        return rawChunk != null && !rawChunk.isEmpty();
    }
}
