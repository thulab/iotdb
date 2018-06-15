package cn.edu.tsinghua.iotdb.udf;

/**
 * @author qmm
 */
public class TimeSpan extends AbstractUDSF {
  private final long span;

  public TimeSpan(long span) {
    this.span = span;
  }

  @Override
  public boolean isCuttingpoint(long time, Comparable<?> value) {
    return time - lastTime >= span;
  }
}
