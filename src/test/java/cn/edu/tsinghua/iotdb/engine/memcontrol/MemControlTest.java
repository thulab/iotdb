package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.StringDataPoint;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static junit.framework.TestCase.assertEquals;

public class MemControlTest {
    private final String FOLDER_HEADER = "src/test/resources";
    private static final String TIMESTAMP_STR = "Time";
    private final String d0 = "root.vehicle.d0";
    private final String d1 = "root.house.d0";
    private final String s0 = "s0";
    private final String s1 = "s1";
    private final String s2 = "s2";
    private final String s3 = "s3";

    private String[] sqls = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "SET STORAGE GROUP TO root.house",
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
            
            "CREATE TIMESERIES root.house.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.house.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.house.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
    };

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;

    TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
    private IoTDB deamon;

    private boolean testFlag = true;
    private boolean exceptionCaught = false;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {

            overflowDataDirPre = config.overflowDataDir;
            fileNodeDirPre = config.fileNodeDir;
            bufferWriteDirPre = config.bufferWriteDir;
            metadataDirPre = config.metadataDir;
            derbyHomePre = config.derbyHome;

            config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
            config.fileNodeDir = FOLDER_HEADER + "/data/digest";
            config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
            config.metadataDir = FOLDER_HEADER + "/data/metadata";
            config.derbyHome = FOLDER_HEADER + "/data/derby";

            BasicMemController.getInstance().setCheckInterval(5 * 1000);  // 5s
            BasicMemController.getInstance().setDangerouseThreshold(2 * TsFileDBConstant.MB);  // force initialize
            BasicMemController.getInstance().setWarningThreshold(1 * TsFileDBConstant.MB);

            deamon = new IoTDB();
            deamon.active();
            insertSQL();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);

            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            FileUtils.deleteDirectory(new File(config.overflowDataDir));
            FileUtils.deleteDirectory(new File(config.fileNodeDir));
            FileUtils.deleteDirectory(new File(config.bufferWriteDir));
            FileUtils.deleteDirectory(new File(config.metadataDir));
            FileUtils.deleteDirectory(new File(config.derbyHome));
            FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));

            config.overflowDataDir = overflowDataDirPre;
            config.fileNodeDir = fileNodeDirPre;
            config.bufferWriteDir = bufferWriteDirPre;
            config.metadataDir = metadataDirPre;
            config.derbyHome = derbyHomePre;
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        // test a huge amount of write causes block
        if(!testFlag)
            return;
        Thread t1 = new Thread(() -> insert(d0));
        Thread t2 = new Thread(() -> insert(d1));
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        assertEquals(exceptionCaught, true);
        assertEquals(BasicMemController.UsageLevel.WARNING, BasicMemController.getInstance().getCurrLevel());

        // test MemControlTread auto flush
        Thread.sleep(10000);
        assertEquals(BasicMemController.UsageLevel.SAFE, BasicMemController.getInstance().getCurrLevel());
    }

    public void insert(String deviceId) {
        try {
            Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = null;
        TSRecord record = new TSRecord(0, deviceId);
        record.addTuple(new IntDataPoint(s0, 0));
        record.addTuple(new LongDataPoint(s1, 0));
        record.addTuple(new FloatDataPoint(s2, 0.0f));
        record.addTuple(new StringDataPoint(s3, new Binary("\"sadasgag\"")));
        long recordMemSize = MemUtils.getTsRecordMemBufferwrite(record);
        long insertCnt = config.memThresholdDangerous / recordMemSize * 2;

        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (int i = 0; i < insertCnt; i++) {
                record.time = i + 1;
                statement.execute(TestUtils.recordToInsert(record));
            }
            statement.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if(e.getMessage().contains("exceeded dangerous threshold"))
                exceptionCaught = true;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
