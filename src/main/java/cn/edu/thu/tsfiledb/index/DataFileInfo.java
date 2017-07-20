package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * The class for storing information of a TsFile data file.
 *
 * @author Jiaye Wu
 */
public class DataFileInfo {

    private long startTime;

    private long endTime;

    private String filePath;

    public DataFileInfo(long startTime, long endTime, String filePath) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.filePath = filePath;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getFilePath() {
        return filePath;
    }

    public List<Pair<Long, Long>> getTimeInterval() {
        List<Pair<Long, Long>> ret = new ArrayList<>(1);
        ret.add(new Pair<>(this.startTime, this.endTime));
        return ret;
    }
}
