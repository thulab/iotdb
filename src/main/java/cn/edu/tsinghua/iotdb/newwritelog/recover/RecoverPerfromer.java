package cn.edu.tsinghua.iotdb.newwritelog.recover;

import cn.edu.tsinghua.iotdb.exception.RecoverException;

public interface RecoverPerfromer {
    void recover() throws RecoverException;
}
