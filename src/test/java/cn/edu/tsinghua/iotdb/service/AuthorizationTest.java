package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class AuthorizationTest {

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
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
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        assertEquals(true, adminStmt.execute("CREATE USER tempuser temppw"));

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

    }
}
