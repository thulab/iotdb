package cn.edu.tsinghua.iotdb.newwritelog;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.RecoverException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.newwritelog.transfer.PhysicalPlanLogTransfer;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.ExclusiveWriteLogNode;
import cn.edu.tsinghua.iotdb.newwritelog.writelognode.WriteLogNode;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
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
    private boolean skip = false;

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
        // this test write 1000000 * 3 logs and report elapsed time
        if (skip)
            return;
        int[] batchSizes = new int[]{100, 500, 1000, 5000, 10000};
        int oldBatchSize = config.flushWalThreshold;
        for (int j = 0; j < batchSizes.length; j++) {
            config.flushWalThreshold = batchSizes[j];
            File tempRestore = new File("testtemp", "restore");
            File tempProcessorStore = new File("testtemp", "processorStore");
            tempRestore.getParentFile().mkdirs();
            tempRestore.createNewFile();
            tempProcessorStore.createNewFile();

            WriteLogNode logNode = new ExclusiveWriteLogNode("root.testLogNode", tempRestore.getPath(), tempProcessorStore.getPath());

            long time = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                        Arrays.asList("1.0", "15", "str", "false"));
                UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
                DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

                logNode.write(bwInsertPlan);
                logNode.write(updatePlan);
                logNode.write(deletePlan);
            }
            logNode.forceSync();
            System.out.println(3000000 + " logs use " + (System.currentTimeMillis() - time) + " ms at batch size " + batchSizes[j]);

            logNode.close();
            tempRestore.delete();
            tempProcessorStore.delete();
        }
        config.flushWalThreshold = oldBatchSize;
    }

    @Test
    public void recoverTest() throws IOException, RecoverException, FileNodeManagerException, PathErrorException, MetadataArgsErrorException {
        // this test write 1000000 * 3 logs , recover from them and report elapsed time
        if (skip)
            return;
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        try {
            MManager.getInstance().setStorageLevelToMTree("root.logTestDevice");
        } catch (PathErrorException ignored) {
        }
        MManager.getInstance().addPathToMTree("root.logTestDevice.s1", TSDataType.DOUBLE.name(), TSEncoding.PLAIN.name(), new String[]{});
        MManager.getInstance().addPathToMTree("root.logTestDevice.s2", TSDataType.INT32.name(), TSEncoding.PLAIN.name(), new String[]{});
        MManager.getInstance().addPathToMTree("root.logTestDevice.s3", TSDataType.TEXT.name(), TSEncoding.PLAIN.name(), new String[]{});
        MManager.getInstance().addPathToMTree("root.logTestDevice.s4", TSDataType.BOOLEAN.name(), TSEncoding.PLAIN.name(), new String[]{});
        WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice", tempRestore.getPath(), tempProcessorStore.getPath());

        for (int i = 0; i < 1000000; i++) {
            InsertPlan bwInsertPlan = new InsertPlan(1, "root.logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                    Arrays.asList("1.0", "15", "str", "false"));
            UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
            DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

            logNode.write(bwInsertPlan);
            logNode.write(updatePlan);
            logNode.write(deletePlan);
        }
        logNode.forceSync();
        long time = System.currentTimeMillis();
        logNode.recover();
        System.out.println(3000000 + " logs use " + (System.currentTimeMillis() - time) + "ms when recovering " );

        logNode.close();
        tempRestore.delete();
        tempProcessorStore.delete();
    }

    @Test
    public void encodeDecodeTest() throws IOException {
        long time = System.currentTimeMillis();
        byte[] bytes3 = null;
        byte[] bytes2 = null;
        byte[] bytes1 = null;

        InsertPlan bwInsertPlan = new InsertPlan(1, "root.logTestDevice", 100, Arrays.asList("s1", "s2", "s3", "s4"),
                Arrays.asList("1.0", "15", "str", "false"));
        UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
        for (int i = 0; i < 20; i++) {
            updatePlan.addInterval(new Pair<Long, Long>(200l, 300l));
        }

        DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));
        for (int i = 0; i < 1000000; i++) {
            bytes1 = PhysicalPlanLogTransfer.operatorToLog(bwInsertPlan);
           bytes2 = PhysicalPlanLogTransfer.operatorToLog(updatePlan);
            bytes3 = PhysicalPlanLogTransfer.operatorToLog(deletePlan);
        }
        System.out.println("3000000 logs encoding use " + (System.currentTimeMillis() - time) + "ms");

        time = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            bwInsertPlan = (InsertPlan) PhysicalPlanLogTransfer.logToOperator(bytes1);
            updatePlan = (UpdatePlan) PhysicalPlanLogTransfer.logToOperator(bytes2);
            deletePlan = (DeletePlan) PhysicalPlanLogTransfer.logToOperator(bytes3);
        }
        System.out.println("3000000 logs decoding use " + (System.currentTimeMillis() - time) + "ms");
    }
}
