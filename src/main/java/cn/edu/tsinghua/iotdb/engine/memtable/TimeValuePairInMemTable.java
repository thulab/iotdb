package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * Created by zhangjinrui on 2018/1/26.
 */
public class TimeValuePairInMemTable extends TimeValuePair implements Comparable {

    public TimeValuePairInMemTable(long timestamp, TsPrimitiveType value) {
        super(timestamp, value);
    }

    @Override
    public int compareTo(Object object) {
        TimeValuePairInMemTable o = (TimeValuePairInMemTable) object;
        if (this.getTimestamp() == o.getTimestamp())
            return 0;
        return this.getTimestamp() < o.getTimestamp() ? -1 : 1;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || !(object instanceof TimeValuePairInMemTable))
            return false;
        TimeValuePairInMemTable o = (TimeValuePairInMemTable) object;
        return o.getTimestamp() == this.getTimestamp();
    }
}