package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.*;
import cn.edu.tsinghua.iotdb.engine.overflow.io.EngineTestHelper;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * @author Liliang
 */

public class MonitorTest {
    private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
    private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

    private FileNodeManager fManager = null;

    private String deltaObjectId = "root.vehicle.d0";
    private String deltaObjectId2 = "root.vehicle.d1";
    private String measurementId = "s0";
    private String measurementId6 = "s6";
    private TSDataType dataType = TSDataType.INT32;

    private String FileNodeDir;
    private String BufferWriteDir;
    private String overflowDataDir;
    private int rowGroupSize;
    private int pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
    private int defaultMaxStringLength = tsconfig.maxStringLength;
    private boolean cachePageData = tsconfig.duplicateIncompletedPage;
    private int pageSize = tsconfig.pageSizeInByte;

    @Before
    public void setUp() throws Exception {
        FileNodeDir = tsdbconfig.fileNodeDir;
        BufferWriteDir = tsdbconfig.bufferWriteDir;
        overflowDataDir = tsdbconfig.overflowDataDir;

        tsdbconfig.fileNodeDir = "filenode" + File.separatorChar;
        tsdbconfig.bufferWriteDir = "bufferwrite";
        tsdbconfig.overflowDataDir = "overflow";
        tsdbconfig.metadataDir = "metadata";
        tsdbconfig.backLoopPeriod = 1;
        tsconfig.duplicateIncompletedPage = true;
        // set rowgroupsize
        tsconfig.groupSizeInByte = 2000;
        tsconfig.pageCheckSizeThreshold = 3;
        tsconfig.pageSizeInByte = 100;
        tsconfig.maxStringLength = 2;
        EngineTestHelper.delete(tsdbconfig.fileNodeDir);
        EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
        EngineTestHelper.delete(tsdbconfig.overflowDataDir);
        EngineTestHelper.delete(tsdbconfig.walFolder);
        EngineTestHelper.delete(tsdbconfig.metadataDir);
        WriteLogManager.getInstance().close();
    }

    @After
    public void tearDown() throws Exception {
        WriteLogManager.getInstance().close();
        MManager.getInstance().flushObjectToFile();
        EngineTestHelper.delete(tsdbconfig.fileNodeDir);
        EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
        EngineTestHelper.delete(tsdbconfig.overflowDataDir);
        EngineTestHelper.delete(tsdbconfig.walFolder);
        EngineTestHelper.delete(tsdbconfig.metadataDir);

        tsdbconfig.fileNodeDir = FileNodeDir;
        tsdbconfig.overflowDataDir = overflowDataDir;
        tsdbconfig.bufferWriteDir = BufferWriteDir;

        tsconfig.groupSizeInByte = rowGroupSize;
        tsconfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
        tsconfig.pageSizeInByte = pageSize;
        tsconfig.maxStringLength = defaultMaxStringLength;
        tsconfig.duplicateIncompletedPage = cachePageData;
    }

    @Test
    public void testFileNodeManagerMonitorAndAddMetadata() {
        fManager = FileNodeManager.getInstance();
        StatMonitor statMonitor = StatMonitor.getInstance();
        statMonitor.activate();
//        statMonitor.registStatistics(fManager.getClass().getName(), fManager);

        // add metadata
        MManager mManager = MManager.getInstance();
        fManager.registStatMetadata();
        HashMap<String, AtomicLong> statParamsHashMap = fManager.getStatParamsHashMap();
        for (String statParam : statParamsHashMap.keySet()) {
            assertEquals(true, mManager.pathExist(
                    MonitorConstants.getStatPrefix() + "write.global/FileNodeManager." + statParam)
            );
        }

        // wait for time second
        try {
            Thread.sleep(5000);
            statMonitor.close();
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Get stat data and test right

        HashMap<String, TSRecord> statHashMap = statMonitor.gatherStatistics();
        Long numInsert = statMonitor.getNumBackLoop();

        String path = fManager.getAllPathForStatistic().get(0);
        int pos = path.lastIndexOf('.');
        TSRecord fTSRecord = statHashMap.get(path.substring(0, pos));
        assertNotEquals(null, fTSRecord);
        for (DataPoint dataPoint : fTSRecord.dataPointList) {
            String m = dataPoint.getMeasurementId();
            Long v = (Long) dataPoint.getValue();
            if (m.contains("Fail")){
                assertEquals(v, new Long(0));
            } else if (m.contains("Points")) {
                assertEquals(v, new Long(numInsert * fManager.getStatParamsHashMap().size()));
            } else {
                assertEquals(v, numInsert);
            }
        }

        try {
            fManager.closeAll();
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
