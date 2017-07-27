package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;

import java.util.List;

/**
 * The class for storing information of a TsFile data file.
 *
 * @author Jiaye Wu
 */
public class DataFileMultiSeriesInfo {

    private String filePath;

    private List<Path> columnPaths;

    private List<Pair<Long, Long>> timeRanges;

    public DataFileMultiSeriesInfo(String filePath) {
        this.filePath = filePath;
    }

    public DataFileMultiSeriesInfo(String filePath, List<Path> columnPaths, List<Pair<Long, Long>> timeRanges) {
        this.filePath = filePath;
        this.columnPaths = columnPaths;
        this.timeRanges = timeRanges;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public List<Path> getColumnPaths() {
        return columnPaths;
    }

    public void setColumnPaths(List<Path> columnPaths) {
        this.columnPaths = columnPaths;
    }

    public List<Pair<Long, Long>> getTimeRanges() {
        return timeRanges;
    }

    public void setTimeRanges(List<Pair<Long, Long>> timeRanges) {
        this.timeRanges = timeRanges;
    }
}
