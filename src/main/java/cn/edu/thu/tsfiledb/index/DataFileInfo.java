package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.io.File;
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

    private File file;

    public DataFileInfo(long startTime, long endTime, File file) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.file = file;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public List<Pair<Long, Long>> getTimeInterval() {
        List<Pair<Long, Long>> ret = new ArrayList<>(1);
        ret.add(new Pair<>(this.startTime, this.endTime));
        return ret;
    }
}
