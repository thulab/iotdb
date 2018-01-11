package cn.edu.tsinghua.iotdb.newwritelog;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.WriteLogNode;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class PerformanceTest {

    private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    private boolean enableWal;

    @Before
    public void setUp() throws Exception {
        enableWal = config.enableWal;
        config.enableWal = true;
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
        config.enableWal = enableWal;
    }

    @Test
    public void writeLogTest() throws IOException {
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.testLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        for(int i = 0; i < 1000000; i++) {
            InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                    Arrays.asList("1.0", "15", "str", "false"));
            UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
            DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

            logNode.write(bwInsertPlan);
            logNode.write(updatePlan);

        }

    }
}
