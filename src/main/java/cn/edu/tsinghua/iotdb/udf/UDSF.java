package cn.edu.tsinghua.iotdb.udf;

/**
 * @author qmm
 */
public interface UDSF {
    boolean isBreakpoint(long time, Comparable<?> value);
}