package cn.edu.tsinghua.iotdb.udf;

/**
 * Created by qmm on 18/3/26.
 */
public class FixedWindowSize extends AbstractUDSF {
    private final long windowSize;
    private boolean windowStart = false;
    private long startTime = -1L;

    public FixedWindowSize(long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public boolean isCuttingpoint(long time, Comparable<?> value) {
        if (!windowStart) {
            startTime = time;
            windowStart = true;
        }

        if (time - startTime >= windowSize) {
            windowStart = false;
            return true;
        }

        return false;
    }
}
