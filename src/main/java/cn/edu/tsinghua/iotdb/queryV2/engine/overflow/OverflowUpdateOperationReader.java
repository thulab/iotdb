package cn.edu.tsinghua.iotdb.queryV2.engine.overflow;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public interface OverflowUpdateOperationReader {

    boolean hasNext();

    OverflowUpdateOperation next();
}
