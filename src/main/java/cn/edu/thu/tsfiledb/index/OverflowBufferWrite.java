package cn.edu.thu.tsfiledb.index;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.IntervalUtils;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Used for KV-match, get overflow data and buffer-write data separately.
 */
public class OverflowBufferWrite {

    private List<Pair<Long, Long>> insertOrUpdateIntervals;

    private long deleteUntil;

    public OverflowBufferWrite(DynamicOneColumnData insert, DynamicOneColumnData update, long deleteUntil, long bufferWriteBeginTime) {
        this.deleteUntil = deleteUntil;

        List<Pair<Long, Long>> insertIntervals = new ArrayList<>();
        if (insert != null) {
            for (int i = 0; i < insert.timeLength; i++) {
                insertIntervals.add(new Pair<>(insert.getTime(i), insert.getTime(i)));
            }
        }
        insertIntervals.add(new Pair<>(bufferWriteBeginTime, Long.MAX_VALUE));
        insertIntervals = IntervalUtils.sortAndMergePair(insertIntervals);
        List<Pair<Long, Long>> updateIntervals = new ArrayList<>();
        if (update != null) {
            for (int i = 0; i < update.timeLength; i += 2) {
                updateIntervals.add(new Pair<>(update.getTime(i), update.getTime(i + 1)));
            }
        }
        insertOrUpdateIntervals = IntervalUtils.union(insertIntervals, updateIntervals);
    }

    public List<Pair<Long, Long>> getInsertOrUpdateIntervals() {
        return insertOrUpdateIntervals;
    }

    public long getDeleteUntil() {
        return deleteUntil;
    }
}
