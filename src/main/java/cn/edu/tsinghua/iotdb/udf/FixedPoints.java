package cn.edu.tsinghua.iotdb.udf;

/**
 * Created by qmm on 18/3/29.
 */
public class FixedPoints extends AbstractUDSF {
    private final long fixedCount;
    private boolean windowStart = false;
    private long curCount = 0L;

    public FixedPoints(long fixedCount) {
        this.fixedCount = fixedCount;
    }

    @Override
    public boolean isCuttingpoint(long time, Comparable<?> value) {
        if (!windowStart) {
            curCount = 0L;
            windowStart = true;
        }

        ++curCount;
        if (curCount >= fixedCount) {
            windowStart = false;
            return true;
        }

        return false;
    }
}
