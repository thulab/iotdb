package cn.edu.tsinghua.iotdb.udf;

/**
 * @author qmm
 */
public class TimeSpan extends AbstractUDSF {
  private final long span;

  public TimeSpan(long lastTime, Comparable<?> value, long span) {
    super(lastTime, value);
    this.span = span;
  }

  @Override
  public boolean isBreakpoint(long time, Comparable<?> value) {
    return time - lastTime > span;
  }
}
