package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthorizationTest {

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(2000);
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void illegalGrantRevokeUserTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        // grant a non-existing user
        boolean caught = false;
        try {
            adminStmt.execute("GRANT USER nulluser PRIVILEGES 'CREATE_TIMESERIES' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant a non-existing privilege
        caught = false;
        try {
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'NOT_A_PRIVILEGE' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // duplicate grant
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        caught = false;
        try {
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant on a illegal path
        caught = false;
        try {
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant admin
        caught = false;
        try {
            adminStmt.execute("GRANT USER root PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // no privilege to grant
        caught = false;
        try {
            userStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke a non-existing privilege
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        caught = false;
        try {
            adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke a non-existing user
        caught = false;
        try {
            adminStmt.execute("REVOKE USER tempuser1 PRIVILEGES 'CREATE_USER' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke on a illegal path
        caught = false;
        try {
            adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke admin
        caught = false;
        try {
            adminStmt.execute("REVOKE USER root PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // no privilege to revoke
        caught = false;
        try {
            userStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant privilege to grant
        caught = false;
        try {
            userStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'GRANT_USER_PRIVILEGE' on root");
        userStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root");

        // grant privilege to revoke
        caught = false;
        try {
            userStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'REVOKE_USER_PRIVILEGE' on root");
        userStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root");
    }

    @Test
    public void createDeleteTimeSeriesTest() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        // grant and revoke the user the privilege to create time series
        boolean caught = false;
        try {
            userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_TIMESERIES' ON root.a");
        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        caught = false;
        try {
            // no privilege to create this one
            userStmt.execute("SET STORAGE GROUP TO root.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            // privilege already exists
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_TIMESERIES' ON root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'CREATE_TIMESERIES' ON root.a");
        caught = false;
        try {
            // no privilege to create this one any more
            userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // the user cannot delete the timeseries now
        caught = false;
        try {
            // no privilege to create this one any more
            userStmt.execute("DELETE TIMESERIES root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // the user can delete the timeseries now
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a");
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.b");
        userStmt.execute("DELETE TIMESERIES root.a.b");

        // revoke the privilege to delete time series
        adminStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("SET STORAGE GROUP TO root.b");
        adminStmt.execute("CREATE TIMESERIES root.b.a WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a");
        userStmt.execute("DELETE TIMESERIES root.b.a");
        caught = false;
        try {
            // no privilege to create this one any more
            userStmt.execute("DELETE TIMESERIES root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminCon.close();
        userCon.close();
    }

    @Test
    public void insertQueryTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_TIMESERIES' ON root.a");
        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        // grant privilege to insert
        boolean caught = false;
        try {
            userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'INSERT_TIMESERIES' on root.a");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");

        // revoke privilege to insert
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'INSERT_TIMESERIES' on root.a");
        caught = false;
        try {
            userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant privilege to query
        caught = false;
        try {
            userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'READ_TIMESERIES' on root.a");
        userStmt.execute("SELECT * from root.a");
        userStmt.getResultSet().close();

        // revoke privilege to query
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'READ_TIMESERIES' on root.a");
        caught = false;
        try {
            userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminCon.close();
        userCon.close();
    }

    @Test
    public void rolePrivilegeTest() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        boolean caught = false;
        try {
            userStmt.execute("CREATE ROLE admin");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("CREATE ROLE admin");
        adminStmt.execute("GRANT ROLE admin PRIVILEGES 'CREATE_TIMESERIES','DELETE_TIMESERIES','READ_TIMESERIES','INSERT_TIMESERIES' on root");
        adminStmt.execute("GRANT admin TO tempuser");

        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("CREATE TIMESERIES root.a.c WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp,b,c) VALUES (1,100,1000)");
        userStmt.execute("DELETE FROM root.a.b WHERE TIME <= 1000000000");
        userStmt.execute("SELECT * FROM root");
        userStmt.getResultSet().close();

        adminStmt.execute("REVOKE ROLE admin PRIVILEGES 'DELETE_TIMESERIES' on root");
        caught = false;
        try {
            userStmt.execute("DELETE FROM root.* WHERE TIME <= 1000000000");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'READ_TIMESERIES' on root");
        adminStmt.execute("REVOKE admin FROM tempuser");
        userStmt.execute("SELECT * FROM root");
        userStmt.getResultSet().close();
        caught = false;
        try {
            userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminCon.close();
        userCon.close();
    }

    @Test @Ignore
    public void authPerformanceTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");
        adminStmt.execute("SET STORAGE GROUP TO root.a");
        int privilegeCnt = 50;
        for(int i = 0; i < privilegeCnt; i++) {
            adminStmt.execute("CREATE TIMESERIES root.a.b" + i + " WITH DATATYPE=INT32,ENCODING=PLAIN");
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'INSERT_TIMESERIES' ON root.a.b" + i);
        }

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        int insertCnt = 200000;
        int batchSize = 5000;
        long time;

        time = System.currentTimeMillis();
        for(int i = 0; i < insertCnt;) {
            for(int j = 0; j < batchSize; j++)
                userStmt.addBatch("INSERT INTO root.a(timestamp, b" + (privilegeCnt - 1) +  ") VALUES (" + (i++ + 1 ) + ", 100)");
            userStmt.executeBatch();
            userStmt.clearBatch();
        }
        System.out.println("User inserted " + insertCnt + " data points used " + (System.currentTimeMillis() - time) + " ms with " + privilegeCnt + " privileges");

        time = System.currentTimeMillis();
        for(int i = 0; i < insertCnt;) {
            for(int j = 0; j < batchSize; j++)
                adminStmt.addBatch("INSERT INTO root.a(timestamp, b0) VALUES (" + (i++ + 1 + insertCnt) + ", 100)");
            adminStmt.executeBatch();
            adminStmt.clearBatch();
        }
        System.out.println("admin inserted " + insertCnt + " data points used " + (System.currentTimeMillis() - time) + " ms with " + privilegeCnt + " privileges");

        adminCon.close();
        userCon.close();
    }

    /*public static void main(String[] args) throws Exception {
        for(int i = 0; i < 10; i++) {
            AuthorizationTest test = new AuthorizationTest();
            test.setUp();
            test.authPerformanceTest();
            test.tearDown();
        }
    }*/
}
