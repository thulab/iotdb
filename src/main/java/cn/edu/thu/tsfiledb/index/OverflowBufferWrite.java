package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.index.utils.IntervalUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Used for KV-match, get overflow data and buffer-write data separately.
 */
public class OverflowBufferWrite {

    private List<Pair<Long, Long>> insertOrUpdateIntervals;

    private long deleteUntil;

    private QueryDataSet bufferWriteData;

    public OverflowBufferWrite(DynamicOneColumnData insert, DynamicOneColumnData update, long deleteUntil, QueryDataSet bufferWriteData) {
        this.deleteUntil = deleteUntil;
        this.bufferWriteData = bufferWriteData;

        List<Pair<Long, Long>> insertIntervals = new ArrayList<>(insert.timeLength);
        for (int i = 0; i < insert.timeLength; i++) {
            insertOrUpdateIntervals.add(new Pair<>(insert.getTime(i), insert.getTime(i)));
        }
        List<Pair<Long, Long>> updateIntervals = new ArrayList<>(update.timeLength / 2);
        for (int i = 0; i < update.timeLength; i += 2) {
            insertOrUpdateIntervals.add(new Pair<>(update.getTime(i), update.getTime(i+1)));
        }
        insertOrUpdateIntervals = IntervalUtils.union(insertIntervals, updateIntervals);
    }

    public List<Pair<Long, Long>> getInsertOrUpdateIntervals() {
        return insertOrUpdateIntervals;
    }

    public long getDeleteUntil() {
        return deleteUntil;
    }

    public QueryDataSet getBufferWriteData() {
        return bufferWriteData;
    }
}
