package cn.edu.tsinghua.iotdb.monitor;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.*;
//import cn.edu.tsinghua.iotdb.engine.overflow.io.EngineTestHelper;
import cn.edu.tsinghua.iotdb.engine.lru.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
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
    private String FileNodeDir;
    private String BufferWriteDir;
    private String overflowDataDir;
    private int rowGroupSize;
    private int pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
    private int defaultMaxStringLength = tsconfig.maxStringLength;
    private boolean cachePageData = tsconfig.duplicateIncompletedPage;
    private int pageSize = tsconfig.pageSizeInByte;
    private StatMonitor statMonitor;

    @Before
    public void setUp() throws Exception {
        // origin value
        // modify stat parameter
        EnvironmentUtils.envSetUp();
        tsdbconfig.enableStatMonitor = true;
        tsdbconfig.backLoopPeriod = 1;
        MetadataManagerHelper.initMetadata();
    }

    @After
    public void tearDown() throws Exception {
        statMonitor.close();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void testFileNodeManagerMonitorAndAddMetadata() {
        fManager = FileNodeManager.getInstance();
        statMonitor = StatMonitor.getInstance();
        fManager.getStatParamsHashMap().forEach((key, value)->value.set(0));
        statMonitor.clearProcessor();
        statMonitor.registStatistics(fManager.getClass().getSimpleName(), fManager);
        // add metadata
        MManager mManager = MManager.getInstance();
//        try {
//            if (!mManager.pathExist("root.stats")) {
//                mManager.setStorageLevelToMTree("root.stats");
//            }
//        } catch (Exception e) {
//            fail(e.getMessage());
//        }

        fManager.registStatMetadata();
        HashMap<String, AtomicLong> statParamsHashMap = fManager.getStatParamsHashMap();
        for (String statParam : statParamsHashMap.keySet()) {
            assertEquals(true, mManager.pathExist(
                    MonitorConstants.getStatPrefix() + "write.global." + statParam)
            );
        }
        statMonitor.activate();
        // wait for time second
        try {
            Thread.sleep(3100);
            statMonitor.close();
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Get stat data and test right

        HashMap<String, TSRecord> statHashMap = fManager.getAllStatisticsValue();
        Long numInsert = statMonitor.getNumInsert();
        Long numPointsInsert = statMonitor.getNumPointsInsert();
        String path = fManager.getAllPathForStatistic().get(0);
        int pos = path.lastIndexOf('.');
        TSRecord fTSRecord = statHashMap.get(path.substring(0, pos));
        System.out.println(fTSRecord.toString());

        assertNotEquals(null, fTSRecord);
        for (DataPoint dataPoint : fTSRecord.dataPointList) {
            String m = dataPoint.getMeasurementId();
            Long v = (Long) dataPoint.getValue();
            if (m.contains("Fail")){
                assertEquals(v, new Long(0));
            } else if (m.contains("Points")) {
//                System.out.println("Measurements");
//                System.out.println(m);
//                System.out.println(numPointsInsert);
                assertEquals(v, numPointsInsert);
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
