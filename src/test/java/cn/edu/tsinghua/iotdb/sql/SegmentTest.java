package cn.edu.tsinghua.iotdb.sql;

import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.executor.OverflowQPExecutor;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import org.junit.Test;

public class SegmentTest {
    @Test
    public void sqlParse() throws Exception {
        long start = System.currentTimeMillis();
        QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());

        PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan("select count(s1) from root.vehicle.d1 where root.vehicle.d1.s1 < 10 and time <= now() segment by timeSpan(60)");
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
