package cn.edu.tsinghua.iotdb.newwritelog.recover;

import cn.edu.tsinghua.iotdb.exception.RecoverException;

public interface RecoverPerformer {
    /**
     * Start the recovery process of the module to which this object belong.
     * @throws RecoverException
     */
    void recover() throws RecoverException;
}
