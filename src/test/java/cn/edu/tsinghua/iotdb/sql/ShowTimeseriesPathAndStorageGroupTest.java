package cn.edu.tsinghua.iotdb.sql;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class ShowTimeseriesPathAndStorageGroupTest {
    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    private static String[] insertSqls = new String[]{
            "SET STORAGE GROUP TO root.ln.wf01.wt01",
            "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, COMPRESSOR = SNAPPY, MAX_POINT_NUMBER = 3"
    };

    public void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : insertSqls) {
                statement.execute(sql);
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

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();

            insertSQL();
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
    public void Test() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            String[] sqls = new String[]{
                    "show timeseries root.ln.wf01.wt01.status", // full path
                    "show timeseries root.ln", // prefix path
                    "show timeseries root.ln.*.wt01", // path with stars
                    "show storage group", // nonexistent timeseries
                    "show timeseries root.a.b", // SHOW TIMESERIES <PATH> only accept single path
                    "show timeseries root.ln,root.ln", // SHOW TIMESERIES <PATH> only accept single path
                    "show timeseries", // not supported in jdbc
                    "show storage group"
            };
            String[] standards = new String[]{"root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n",

                    "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n" +
                            "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n",

                    "root.ln.wf01.wt01.status,root.ln.wf01.wt01,BOOLEAN,PLAIN,\n" +
                            "root.ln.wf01.wt01.temperature,root.ln.wf01.wt01,FLOAT,RLE,\n",

                    "root.ln.wf01.wt01,\n",

                    "",

                    "",

                    "", // used as a placeholder because this sql is asserted in the 'catch' clause

                    "root.ln.wf01.wt01,\n"

            };
            for (int n = 0; n < sqls.length; n++) {
                String sql = sqls[n];
                String standard = standards[n];
                StringBuilder builder = new StringBuilder();
                try {
                    boolean hasResultSet = statement.execute(sql);
                    if (hasResultSet) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        while (resultSet.next()) {
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                builder.append(resultSet.getString(i)).append(",");
                            }
                            builder.append("\n");
                        }
                    }
                    Assert.assertEquals(builder.toString(), standard);
                } catch (SQLException e) {
                    // assert specially for sql "show timeseries"
                    Assert.assertEquals(e.toString(), "java.sql.SQLException: Error format of 'SHOW TIMESERIES <PATH>'");
                }
            }
            statement.close();
        } finally {
            connection.close();
        }
    }


}
