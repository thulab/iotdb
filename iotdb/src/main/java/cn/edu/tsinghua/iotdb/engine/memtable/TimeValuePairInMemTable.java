package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.datatype.TsPrimitiveType;


public class TimeValuePairInMemTable extends TimeValuePair implements Comparable<TimeValuePairInMemTable> {

    public TimeValuePairInMemTable(long timestamp, TsPrimitiveType value) {
        super(timestamp, value);
    }

    @Override
    public int compareTo(TimeValuePairInMemTable o) {
        return Long.compare(this.getTimestamp(), o.getTimestamp());
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof TimeValuePairInMemTable))
            return false;
        TimeValuePairInMemTable o = (TimeValuePairInMemTable) object;
        return o.getTimestamp() == this.getTimestamp();
    }
}