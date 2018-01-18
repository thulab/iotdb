package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class OverflowSeriesDataSource {
    private List<OverflowInsertFile> overflowInsertFileList;
    private SeriesChunk memSeriesChunk;
    private UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries;

    public List<OverflowInsertFile> getOverflowInsertFileList() {
        return overflowInsertFileList;
    }

    public void setOverflowInsertFileList(List<OverflowInsertFile> overflowInsertFileList) {
        this.overflowInsertFileList = overflowInsertFileList;
    }

    public SeriesChunk getMemSeriesChunk() {
        return memSeriesChunk;
    }

    public void setMemSeriesChunk(SeriesChunk memSeriesChunk) {
        this.memSeriesChunk = memSeriesChunk;
    }

    public UpdateDeleteInfoOfOneSeries getUpdateDeleteInfoOfOneSeries() {
        return updateDeleteInfoOfOneSeries;
    }

    public void setUpdateDeleteInfoOfOneSeries(UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries) {
        this.updateDeleteInfoOfOneSeries = updateDeleteInfoOfOneSeries;
    }
}
