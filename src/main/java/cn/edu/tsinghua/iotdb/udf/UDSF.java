package cn.edu.tsinghua.iotdb.udf;

/**
 * @author qmm
 */
public interface UDSF {
    boolean isCuttingpoint(long time, Comparable<?> value);
}