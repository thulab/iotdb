package cn.edu.thu.tsfiledb.index.common;

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

    private DynamicOneColumnData insert;

    private DynamicOneColumnData update;

    private long deleteUntil;

    private long bufferWriteBeginTime;

    public OverflowBufferWrite(DynamicOneColumnData insert, DynamicOneColumnData update, long deleteUntil, long bufferWriteBeginTime) {
        this.insert = insert;
        this.update = update;
        this.deleteUntil = deleteUntil;
        this.bufferWriteBeginTime = bufferWriteBeginTime;
    }

    public List<Pair<Long, Long>> getInsertOrUpdateIntervals(int lenQ) {
        List<Pair<Long, Long>> insertIntervals = new ArrayList<>();
        if (insert != null) {
            for (int i = 0; i < insert.timeLength; i++) {
                insertIntervals.add(new Pair<>(insert.getTime(i) - lenQ + 1, insert.getTime(i) + lenQ - 1));
            }
        }
        if (bufferWriteBeginTime < Long.MAX_VALUE) {
            insertIntervals.add(new Pair<>(bufferWriteBeginTime, Long.MAX_VALUE));
            insertIntervals = IntervalUtils.sortAndMergePair(insertIntervals);
        }
        List<Pair<Long, Long>> updateIntervals = new ArrayList<>();
        if (update != null) {
            for (int i = 0; i < update.timeLength; i += 2) {
                updateIntervals.add(new Pair<>(update.getTime(i) - lenQ + 1, update.getTime(i + 1) + lenQ - 1));
            }
        }
        return IntervalUtils.union(insertIntervals, updateIntervals);
    }

    public long getDeleteUntil() {
        return deleteUntil;
    }
}
