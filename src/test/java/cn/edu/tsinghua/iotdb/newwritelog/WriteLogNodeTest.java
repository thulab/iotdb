package cn.edu.tsinghua.iotdb.newwritelog;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.WriteLogNode;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.query.engine.AggregateEngine;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;

public class WriteLogNodeTest {

    TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    boolean enableWal;

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
    public void testWriteLog() throws IOException {
        // this test use a dummy write log node to write a few logs and flush them
        // then read the logs from file
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("TestLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        PhysicalPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        PhysicalPlan ofInsertPlan = new InsertPlan(2, "logTestDevice", 50, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        PhysicalPlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        PhysicalPlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(ofInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        logNode.forceSync();

        File walFile = new File(config.walFolder + File.separator + "TestLogNode" + File.separator + "wal");
        assertTrue(walFile.exists());

        RandomAccessFile raf = new RandomAccessFile(walFile, "r");
        byte[] buffer = new byte[10 * 1024 * 1024];
        int logSize = 0;


        logNode.close();
        tempRestore.delete();
        tempProcessorStore.delete();
    }
}
