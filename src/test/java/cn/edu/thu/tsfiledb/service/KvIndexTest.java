package cn.edu.thu.tsfiledb.service;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class KvIndexTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KvIndexTest.class);
    private final String FOLDER_HEADER = "src/test/resources";
    private static final String TIMESTAMP_STR = "Time";
    private final String d0s0 = "root.vehicle.d0.s0";
    private final String d0s1 = "root.vehicle.d0.s1";
    private final String d0s2 = "root.vehicle.d0.s2";
    private final String d0s3 = "root.vehicle.d0.s3";
    private static final String INSERT_DATA_TEMPLATE = "insert into root.vehicle.%s(timestamp,%s) values (%s,%s)";

    private String[] sqls = new String[]{
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=DOUBLE, ENCODING=RLE",
            "SET STORAGE GROUP TO root.vehicle",
    };

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;
    private String indexHomePre;
    private int length = 20000;

    private Daemon deamon;
    private Connection connection = null;

    @Before
    public void setUp() throws Exception {
        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");

        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        overflowDataDirPre = config.overflowDataDir;
        fileNodeDirPre = config.fileNodeDir;
        bufferWriteDirPre = config.bufferWriteDir;
        metadataDirPre = config.metadataDir;
        derbyHomePre = config.derbyHome;
        indexHomePre = config.indexFileDir;

        config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
        config.fileNodeDir = FOLDER_HEADER + "/data/digest";
        config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
        config.metadataDir = FOLDER_HEADER + "/data/metadata";
        config.derbyHome = FOLDER_HEADER + "/data/derby";
        config.indexFileDir = FOLDER_HEADER + "/data/index";
        config.walFolder = FOLDER_HEADER + "/data/wals";
        deamon = new Daemon();
        deamon.active();
    }

    // @After
    public void tearDown() throws Exception {
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

    private void insertSQL() throws ClassNotFoundException, SQLException {
        try {
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }

            int s0Value = 10;
            double s1Value = 10.0;
            for (int i = 1; i < length; i++) {
                if (i % 10000 == 0) {
                    LOGGER.info("{}", i);
                }

                if (i >= 9000 && i <= 12000) {
                    statement.execute(String.format(INSERT_DATA_TEMPLATE, "d0", "s0", i, 1000));
                    statement.execute(String.format(INSERT_DATA_TEMPLATE, "d0", "s1", i, 1000.0));
                    continue;
                }
                statement.execute(String.format(INSERT_DATA_TEMPLATE, "d0", "s0", i, s0Value));
                statement.execute(String.format(INSERT_DATA_TEMPLATE, "d0", "s1", i, s1Value));
                s0Value += 10;
                s1Value += 10.0;
            }

            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        Thread.sleep(5000);

        connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

        try {
            insertSQL();
            selectAllSQLTest();
        } finally {
            if (connection != null)
                connection.close();
        }
    }

    private void selectAllSQLTest() throws ClassNotFoundException, SQLException {
        try {
            Statement statement = connection.createStatement();
//            try {
//                boolean hasResultSet = statement.execute("select index subm(root.vehicle.d0.s0, root.vehicle.d0.s1, 10000, 11000, 5.0) where time < 150000");
//            } catch (Exception e) {
//                Assert.assertEquals("The timeseries root.vehicle.d0.s0 hasn't been indexed", e.getMessage());
//            }

            statement.execute("CREATE INDEX ON root.vehicle.d0.s0 USING kv-match");
            statement.execute("close");
            try {
                boolean hasResultSet = statement.execute("select index subm(root.vehicle.d0.s0, root.vehicle.d0.s1, 10000, 11000, 50.0) where time < 150000");
                if (hasResultSet) {
                    ResultSet resultSet = statement.getResultSet();
                    int cnt = 0;
                    while (resultSet.next()) {
                        String ans = resultSet.getString(0) + "," + resultSet.getString(1) +
                                "," + resultSet.getString(2) + "," + resultSet.getString(3);
                        System.out.println("==========" + ans);
                        //Assert.assertEquals(ans, retArray[cnt]);
                        cnt++;
                    }
                    // Assert.assertEquals(0, cnt);
                }
            } catch (Exception e) {
                Assert.assertEquals("The timeseries root.vehicle.d0.s0 hasn't been indexed", e.getMessage());
            }

            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
