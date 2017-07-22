package cn.edu.thu.tsfiledb.index.utils;


import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;

/**
 * Used for kvmatch-index, get overflow data and bufferwrite data separately.
 */
public class OverflowBufferWrite {
    public DynamicOneColumnData insert;
    public DynamicOneColumnData update;
    public long deleteMaxLength;
    public QueryDataSet bufferWriteData;

    public OverflowBufferWrite(DynamicOneColumnData insert, DynamicOneColumnData update, long deleteMaxLength, QueryDataSet bufferWriteData) {
        this.insert = insert;
        this.update = update;
        this.deleteMaxLength = deleteMaxLength;
        this.bufferWriteData = bufferWriteData;
    }
}
