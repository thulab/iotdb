package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.tsfile.read.common.Path;

import java.util.List;


public class GlobalSortedSeriesDataSource {
    private Path seriesPath;

    // sealed tsfile
    private List<IntervalFileNode> sealedTsFiles;

    // unsealed tsfile
    private UnsealedTsFile unsealedTsFile;

    // seq mem-table
    private RawSeriesChunk rawSeriesChunk;

    public GlobalSortedSeriesDataSource(Path seriesPath, List<IntervalFileNode> sealedTsFiles,
                                        UnsealedTsFile unsealedTsFile, RawSeriesChunk rawSeriesChunk) {
        this.seriesPath = seriesPath;
        this.sealedTsFiles = sealedTsFiles;
        this.unsealedTsFile = unsealedTsFile;

        this.rawSeriesChunk = rawSeriesChunk;
    }

    public boolean hasSealedTsFiles() {
        return sealedTsFiles != null;
    }

    public List<IntervalFileNode> getSealedTsFiles() {
        return sealedTsFiles;
    }

    public boolean hasUnsealedTsFile() {
        return unsealedTsFile != null;
    }

    public UnsealedTsFile getUnsealedTsFile() {
        return unsealedTsFile;
    }

    public boolean hasRawSeriesChunk() {
        return rawSeriesChunk != null;
    }

    public RawSeriesChunk getRawSeriesChunk() {
        return rawSeriesChunk;
    }


    public void setSealedTsFiles(List<IntervalFileNode> sealedTsFiles) {
        this.sealedTsFiles = sealedTsFiles;
    }

    public void setUnsealedTsFile(UnsealedTsFile unsealedTsFile) {
        this.unsealedTsFile = unsealedTsFile;
    }

    public void setRawSeriesChunk(RawSeriesChunk rawSeriesChunk) {
        this.rawSeriesChunk = rawSeriesChunk;
    }

    public void setSeriesPath(Path seriesPath) {
        this.seriesPath = seriesPath;
    }

    public Path getSeriesPath() {
        return seriesPath;
    }

}
