package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.*;

import static cn.edu.tsinghua.iotdb.service.TestUtils.*;
import static org.junit.Assert.*;


public class LockMaxTest {

    private static final String TIMESTAMP_STR = "Time";
    private static final int DEVICE_NUM = 100;

    private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};
    private static String[] booleanValue = new String[]{"true", "false"};

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    private int maxNumberOfPointsInPage;
    private int pageSizeInByte;
    private int groupSizeInByte;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();

            // use small page setting
            // origin value
            maxNumberOfPointsInPage = tsFileConfig.maxNumberOfPointsInPage;
            pageSizeInByte = tsFileConfig.pageSizeInByte;
            groupSizeInByte = tsFileConfig.groupSizeInByte;
            // new value
            tsFileConfig.maxNumberOfPointsInPage = 1000;
            tsFileConfig.pageSizeInByte = 1024 * 1024 * 150;
            tsFileConfig.groupSizeInByte = 1024 * 1024 * 1000;

            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);
            //recovery value
            tsFileConfig.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
            tsFileConfig.pageSizeInByte = pageSizeInByte;
            tsFileConfig.groupSizeInByte = groupSizeInByte;
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException, FileNotFoundException {
        PrintStream ps = new PrintStream(new FileOutputStream("src/test/resources/ha.txt"));
        System.setOut(ps);

        if (testFlag) {
            Thread.sleep(5000);
            insertSQL();
            Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            aggregationWithoutFilterTest();
            connection.close();
        }
    }


    private void aggregationWithoutFilterTest() throws ClassNotFoundException, SQLException {

        String sql = "select max_value(s0) from root.vehicle.*";
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            boolean hasResultSet = statement.execute(sql);
            Assert.assertTrue(hasResultSet);
            ResultSet resultSet = statement.getResultSet();
            int cnt = 0;
            while (resultSet.next()) {
                String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value("root.vehicle.d1.s0"));
                System.out.println("+++++++++++++　" +ans);
                cnt++;
            }
            assertEquals(1, cnt);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();

            statement.execute("SET STORAGE GROUP TO root.vehicle");

            for (int i = 0; i < DEVICE_NUM; i++) {
                String sql = String.format("CREATE TIMESERIES root.vehicle.d%s.s0 WITH DATATYPE=INT32, ENCODING=RLE", i);
                //System.out.println(sql);
                statement.execute(sql);
            }

            // insert large amount of data    time range : 3000 ~ 13600
            int time = 1000;
            for (time = 3000; time < 3200; time++) {
                for (int i = 0; i < DEVICE_NUM; i++) {
                    String sql = String.format("insert into root.vehicle.d%s(timestamp,s0) values(%s,%s)", i, time, time%10);
                    //System.out.println("---------------insert " + sql);
                    statement.execute(sql);
                }
            }

            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
