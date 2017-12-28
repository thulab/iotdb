package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.jdbc.TsfileSQLException;
import cn.edu.tsinghua.iotdb.qp.physical.index.KvMatchIndexQueryPlan;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Just used for integration test.
 */
public class KVIndexTest {
    private final String FOLDER_HEADER = "src/test/resources";
    private static final String TIMESTAMP_STR = "Time";
    private int maxOpenFolderPre;

    private String count(String path) {
        return String.format("count(%s)", path);
    }

    private String[][] sqls = new String[][]{
            {"SET STORAGE GROUP TO root.vehicle.d0"},
            {"SET STORAGE GROUP TO root.vehicle.d1"},
            {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE"},
            {"CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE"},
//          create the first file of sensor s0
            {"insert into root.vehicle.d0(timestamp,s0) values(1,101)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(2,102)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(3,103)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(4,104)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(5,105)"},
//          create index for d0.s0
            {"create index on root.vehicle.d0.s0 using kvindex with window_length=2, since_time=0"},
//          close the first file of d1.s0 which closes d0.s0 due to the threshold number of existing Filenodes is 1
            {"insert into root.vehicle.d1(timestamp,s0) values(5,102)"},
//          create the second file of sensor s0
            {"insert into root.vehicle.d0(timestamp,s0) values(6,106)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(7,107)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(8,108)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(9,109)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(10,110)"},

//          close the second file of d0.s0
            {"insert into root.vehicle.d1(timestamp,s0) values(6,102)"},
//          create the thrid file of d0.s0 which is unclosed.
            {"insert into root.vehicle.d0(timestamp,s0) values(11,111)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(12,112)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(13,113)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(14,114)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(15,115)"},
//          update the file of d0.s0
            {"update root.vehicle SET d0.s0 = 33333 WHERE time >= 6 and time <= 7"},
            {"insert into root.vehicle.d0(timestamp,s0) values(7,102)"},
//          query kvindex within a single file
            {
                    "select kvindex(root.vehicle.d0.s0, 2, 5, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "0,2,5,0.0",
            },
            {
                    "select kvindex(root.vehicle.d0.s0, 1, 4, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "0,1,4,0.0",
            },
//          query kvindex across multiple files involving Overflow file.
            {
                    "select kvindex(root.vehicle.d0.s0, 4, 7, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "0,4,7,0.0",
            },
//          merge
            {"merge"},
//          query index in a single file after merging operation
            {
                    "select kvindex(root.vehicle.d0.s0, 2, 5, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "0,2,5,0.0",
            },
            {
                    "select kvindex(root.vehicle.d0.s0, 3, 5, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "0,3,5,0.0",
            },
//          query kvindex across multiple files
            {
                    "select kvindex(root.vehicle.d0.s0, 5, 8, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "0,5,8,0.0",
            },
//          drop the index
            {"drop index kvindex on root.vehicle.d0.s0"},
//          query the dropped index will throw exception
            {
                    "select kvindex(root.vehicle.d0.s0, 6, 9, 0.0, 1.0, 0.0) from root.vehicle.d0.s0",
                    "The timeseries root.vehicle.d0.s0 hasn't been indexed.",
            },

    };

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            maxOpenFolderPre = config.maxOpenFolder;
            config.maxOpenFolder = 1;
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(500);
            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            config.maxOpenFolder = maxOpenFolderPre;
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        if (testFlag) {
            Thread.sleep(500);
            executeSQL();
        }
    }

    private void executeSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            for (String[] sqlRet : sqls) {
                String sql = sqlRet[0];
                System.out.println("test-sql\t" + sql);
                if ("".equals(sql))
                    return;
                if (sqlRet.length == 1) {
                    //长度1，non-query语句
                    connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    statement.close();
                } else {
                    //长度2，query语句，第二项是结果
                    query(sql, sqlRet);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void query(String querySQL, String[] retArray) throws ClassNotFoundException,
            SQLException {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            try {
                boolean hasResultSet = statement.execute(querySQL);
                Assert.assertTrue(hasResultSet);
                if (hasResultSet) {
                    ResultSet resultSet = statement.getResultSet();
                    int cnt = 1;
                    while (resultSet.next()) {
                        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2)
                                + "," + resultSet.getString(3)
                                + "," + resultSet.getString(4);
                        if (!retArray[cnt].equals(ans))
                            Assert.assertEquals(retArray[cnt], ans);
                        cnt++;
                        if (cnt > retArray.length)
                            Assert.fail();
                    }
                    if (retArray.length != cnt)
                        Assert.assertEquals(retArray.length, cnt);
                }
            } catch (TsfileSQLException e) {
                Assert.assertEquals(retArray[1], e.getMessage());

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
