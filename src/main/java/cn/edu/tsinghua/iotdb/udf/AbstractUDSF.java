package cn.edu.tsinghua.iotdb.udf;

/**
 * @author qmm
 */
public class AbstractUDSF implements UDSF {
    protected long lastTime;
    protected Comparable<?> lastValue;

    public AbstractUDSF(long lastTime, Comparable<?> lastValue) {
        this.lastTime = lastTime;
        this.lastValue = lastValue;
    }

    @Override
    public boolean isBreakpoint(long time, Comparable<?> value) {
        return false;
    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public Comparable<?> getLastValue() {
        return lastValue;
    }

    public void setLastValue(Comparable<?> lastValue) {
        this.lastValue = lastValue;
    }
}
