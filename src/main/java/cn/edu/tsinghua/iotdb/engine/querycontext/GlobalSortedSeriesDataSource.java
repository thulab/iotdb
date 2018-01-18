package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;

import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class GlobalSortedSeriesDataSource {
    private List<IntervalFileNode> sealedTsFiles;
    private UnsealedTsFile unsealedTsFile;
    private SeriesChunk memSeriesChunk;

    public boolean hasUnsealedTsFile() {
        return unsealedTsFile != null;
    }

    public List<IntervalFileNode> getSealedTsFiles() {
        return sealedTsFiles;
    }

    public void setSealedTsFiles(List<IntervalFileNode> sealedTsFiles) {
        this.sealedTsFiles = sealedTsFiles;
    }

    public UnsealedTsFile getUnsealedTsFile() {
        return unsealedTsFile;
    }

    public void setUnsealedTsFile(UnsealedTsFile unsealedTsFile) {
        this.unsealedTsFile = unsealedTsFile;
    }

    public SeriesChunk getMemSeriesChunk() {
        return memSeriesChunk;
    }

    public void setMemSeriesChunk(SeriesChunk memSeriesChunk) {
        this.memSeriesChunk = memSeriesChunk;
    }
}
