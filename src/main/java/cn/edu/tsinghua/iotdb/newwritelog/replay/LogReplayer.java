package cn.edu.tsinghua.iotdb.newwritelog.replay;

import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public interface LogReplayer {
    void replay(PhysicalPlan plan) throws ProcessorException;
}
