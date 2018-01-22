package cn.edu.tsinghua.iotdb.newwritelog;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.newwritelog.lognodemanager.MultiFileLogNodeManager;
import cn.edu.tsinghua.iotdb.newwritelog.lognodemanager.WriteLogNodeManager;
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

import static junit.framework.TestCase.assertTrue;

public class WriteLogNodeManagerTest {
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
    public void autoSyncTest() throws IOException, InterruptedException {
        // this test check that nodes in a manager will sync periodically.
        int flushWalPeriod = config.flushWalThreshold;
        config.flushWalPeriodInMs = 10000;
        File tempRestore = File.createTempFile("managerTest", "restore");
        File tempProcessorStore = File.createTempFile("managerTest", "processorStore");

        WriteLogNodeManager manager = MultiFileLogNodeManager.getInstance();
        WriteLogNode logNode = manager.getNode("root.managerTest", tempRestore.getPath(), tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        File walFile = new File(logNode.getLogDirectory() + File.separator + "wal");
        assertTrue(!walFile.exists());

        Thread.sleep(config.flushWalPeriodInMs + 1000);
        assertTrue(walFile.exists());

        logNode.delete();
        config.flushWalPeriodInMs = flushWalPeriod;
        tempRestore.delete();
        tempProcessorStore.delete();
    }
}
