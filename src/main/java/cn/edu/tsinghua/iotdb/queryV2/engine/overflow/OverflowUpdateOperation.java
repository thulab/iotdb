package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class OverflowUpdateOperation {
    private long leftBound;
    private long rightBound;
    private TsPrimitiveType value;

    public OverflowUpdateOperation(long leftBound, long rightBound, TsPrimitiveType value) {
        this.leftBound = leftBound;
        this.rightBound = rightBound;
        this.value = value;
    }

    public long getLeftBound() {
        return leftBound;
    }

    public void setLeftBound(long leftBound) {
        this.leftBound = leftBound;
    }

    public long getRightBound() {
        return rightBound;
    }

    public void setRightBound(long rightBound) {
        this.rightBound = rightBound;
    }

    public TsPrimitiveType getValue() {
        return value;
    }

    public void setValue(TsPrimitiveType value) {
        this.value = value;
    }
}
