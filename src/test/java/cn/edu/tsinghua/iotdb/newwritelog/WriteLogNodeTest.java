package cn.edu.tsinghua.iotdb.newwritelog;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class WriteLogNodeTest {

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
    public void testWriteLogAndSync() throws IOException {
        // this test use a dummy write log node to write a few logs and flush them
        // then read the logs from file
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("TestLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        logNode.forceSync();

        File walFile = new File(config.walFolder + File.separator + "TestLogNode" + File.separator + "wal");
        assertTrue(walFile.exists());

        RandomAccessFile raf = new RandomAccessFile(walFile, "r");
        byte[] buffer = new byte[10 * 1024 * 1024];
        int logSize = 0;
        logSize = raf.readInt();
        raf.read(buffer, 0, logSize);
        InsertPlan bwInsertPlan2 = (InsertPlan) PhysicalPlanLogTransfer.logToOperator(buffer);
        assertEquals(bwInsertPlan.getMeasurements(), bwInsertPlan2.getMeasurements());
        assertEquals(bwInsertPlan.getTime(), bwInsertPlan2.getTime());
        assertEquals(bwInsertPlan.getValues(), bwInsertPlan2.getValues());
        assertEquals(bwInsertPlan.getPaths(), bwInsertPlan2.getPaths());
        assertEquals(bwInsertPlan.getDeltaObject(), bwInsertPlan2.getDeltaObject());

        logSize = raf.readInt();
        raf.read(buffer, 0, logSize);
        UpdatePlan updatePlan2 = (UpdatePlan) PhysicalPlanLogTransfer.logToOperator(buffer);
        assertEquals(updatePlan.getPath(), updatePlan2.getPath());
        assertEquals(updatePlan.getIntervals(), updatePlan2.getIntervals());
        assertEquals(updatePlan.getValue(), updatePlan2.getValue());
        assertEquals(updatePlan.getPaths(), updatePlan2.getPaths());

        logSize = raf.readInt();
        raf.read(buffer, 0, logSize);
        DeletePlan deletePlan2 = (DeletePlan) PhysicalPlanLogTransfer.logToOperator(buffer);
        assertEquals(deletePlan.getDeleteTime(), deletePlan2.getDeleteTime());
        assertEquals(deletePlan.getPaths(), deletePlan2.getPaths());

        raf.close();
        logNode.close();
        tempRestore.delete();
        tempProcessorStore.delete();
    }

    @Test
    public void testNotifyFlush() throws IOException {
        // this test write a few logs and sync them
        // then call notifyStartFlush() and notifyEndFlush() to delete old file
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("TestLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);
        logNode.write(deletePlan);

        logNode.forceSync();

        File walFile = new File(config.walFolder + File.separator + "TestLogNode" + File.separator + "wal");
        assertTrue(walFile.exists());

        logNode.notifyStartFlush();
        File oldWalFile = new File(config.walFolder + File.separator + "TestLogNode" + File.separator + "wal-old");
        assertTrue(oldWalFile.exists());
        assertTrue(oldWalFile.length() > 0);

        logNode.notifyEndFlush(null);
        assertTrue(!oldWalFile.exists());
        assertTrue(walFile.exists());
        assertEquals(0, walFile.length());

        logNode.close();
        tempRestore.delete();
        tempProcessorStore.delete();
    }

    @Test
    public void testSyncThreshold() throws IOException {
        // this test check that if more logs than threshold are written, a sync will be triggered.
        int flushWalThreshold = config.flushWalThreshold;
        config.flushWalThreshold = 3;
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("TestLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

        InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        DeletePlan deletePlan = new DeletePlan(50,  new Path("root.logTestDevice.s1"));

        logNode.write(bwInsertPlan);
        logNode.write(updatePlan);

        File walFile = new File(config.walFolder + File.separator + "TestLogNode" + File.separator + "wal");
        assertTrue(!walFile.exists());

        logNode.write(deletePlan);
        assertTrue(walFile.exists());

        logNode.close();
        tempRestore.delete();
        tempProcessorStore.delete();
        config.flushWalThreshold = flushWalThreshold;
    }

}
