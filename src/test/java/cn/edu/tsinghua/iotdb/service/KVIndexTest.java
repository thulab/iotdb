package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.jdbc.TsfileSQLException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.physical.index.KvMatchIndexQueryPlan;
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
//          s0第一个文件
            {"insert into root.vehicle.d0(timestamp,s0) values(1,101)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(2,102)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(3,103)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(4,104)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(5,105)"},
//          创建索引
            {"create index on root.vehicle.d0.s0 using kvindex with window_length=2, since_time=0"},
//          强行切断d0.s0，生成d1.s0文件
            {"insert into root.vehicle.d1(timestamp,s0) values(5,102)"},
//          s0第二个文件
            {"insert into root.vehicle.d0(timestamp,s0) values(6,106)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(7,107)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(8,108)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(9,109)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(10,110)"},

//          强行切断d0.s0，生成第二个d1.s0文件
            {"insert into root.vehicle.d1(timestamp,s0) values(6,102)"},
//          s0第三个文件，处于未关闭状态
            {"insert into root.vehicle.d0(timestamp,s0) values(11,111)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(12,112)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(13,113)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(14,114)"},
            {"insert into root.vehicle.d0(timestamp,s0) values(15,115)"},
//          修改d2.s0，强行切断d0.s0，生成第三个d0.s0文件
            {"update root.vehicle SET d0.s0 = 33333 WHERE time >= 6 and time <= 7"},
            {"insert into root.vehicle.d0(timestamp,s0) values(7,102)"},
//          单文件索引查询
//            {
//                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 4, 7, 0.0, 1.0, 0.0) from root" +
//                            ".vehicle.d0.s0",
//                    "0,4,7,0.0",
//            },
//            {
//                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 2, 5, 0.0, 1.0, 0.0) from root
// .vehicle.d0.s0",
//                    "0,2,5,0.0",
//            },
            {
                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 1, 4, 0.0, 1.0, 0.0) from root" +
                            ".indextest.d0.s0",
                    "0,1,4,0.0",
            },
//          跨文件索引，涉及到Overflow的查询

//          merge操作
            {"merge"},
//          单文件索引查询
            {
                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 2, 5, 0.0, 1.0, 0.0) from root" +
                            ".vehicle.d0.s0",
                    "0,2,5,0.0",
            },
            {
                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 3, 5, 0.0, 1.0, 0.0) from root" +
                            ".vehicle.d0.s0",
                    "0,3,5,0.0",
            },

//          跨文件索引，涉及到Overflow的查询
            {
                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 5, 8, 0.0, 1.0, 0.0) from root" +
                            ".vehicle.d0.s0",
                    "0,5,8,0.0",
            },
//          删除索引
            {"drop index kvindex on root.vehicle.d0.s0"},
////          再次查询
            {
                    "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 6, 9, 0.0, 1.0, 0.0) from root" +
                            ".vehicle.d0.s0",
                    "0,1,4,0.0",
            },

    };

    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;
    private String walFolderPre;
    private String indexFileDirPre;

    private IoTDB deamon;

    private boolean testFlag = true;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            Thread.sleep(1000);

            overflowDataDirPre = config.overflowDataDir;
            fileNodeDirPre = config.fileNodeDir;
            bufferWriteDirPre = config.bufferWriteDir;
            metadataDirPre = config.metadataDir;
            derbyHomePre = config.derbyHome;
            maxOpenFolderPre = config.maxOpenFolder;
            walFolderPre = config.walFolder;
            indexFileDirPre = config.indexFileDir;

            config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
            config.fileNodeDir = FOLDER_HEADER + "/data/digest";
            config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
            config.metadataDir = FOLDER_HEADER + "/data/metadata";
            config.derbyHome = FOLDER_HEADER + "/data/derby";
            config.walFolder = FOLDER_HEADER + "/data/wals";
            config.indexFileDir = FOLDER_HEADER + "/data/index";
            config.maxOpenFolder = 1;
            clearDir(config);
            Thread.sleep(1000);
            MManager.getInstance().clear();
            deamon = new IoTDB();
            deamon.active();
        }
    }

    private void clearDir(TsfileDBConfig config) throws IOException {
        FileUtils.deleteDirectory(new File(config.overflowDataDir));
        FileUtils.deleteDirectory(new File(config.fileNodeDir));
        FileUtils.deleteDirectory(new File(config.bufferWriteDir));
        FileUtils.deleteDirectory(new File(config.metadataDir));
//        FileUtils.deleteDirectory(new File(config.derbyHome));
        FileUtils.deleteDirectory(new File(config.walFolder));
        FileUtils.deleteDirectory(new File(config.indexFileDir));
//        FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);

            TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
            clearDir(config);
            config.overflowDataDir = overflowDataDirPre;
            config.fileNodeDir = fileNodeDirPre;
            config.bufferWriteDir = bufferWriteDirPre;
            config.metadataDir = metadataDirPre;
            config.derbyHome = derbyHomePre;
            config.maxOpenFolder = maxOpenFolderPre;
            config.walFolder = walFolderPre;
            config.indexFileDir = indexFileDirPre;
        }
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException, FileNodeManagerException,
            IOException {
        if (testFlag) {
            Thread.sleep(5000);
            executeSQL();
        }
//        clearDir(TsfileDBDescriptor.getInstance().getConfig());
//        deamon = new IoTDB();
//        deamon.active();
//        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
//        Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
//        Statement statement = connection.createStatement();
//        statement.close();
//        connection.close();
//        deamon.stop();
//        clearDir(TsfileDBDescriptor.getInstance().getConfig());
//        System.out.println("first pass finished");
//        deamon = new IoTDB();
//        deamon.active();
//        Thread.sleep(2000);
//        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
//        connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
//        statement = connection.createStatement();
//        statement.close();
//        connection.close();
//        deamon.stop();
    }

    private void executeSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            for (String[] sqlRet : sqls) {
                String sql = sqlRet[0];
                System.out.println("testtest-sql\t" + sql);
                if ("".equals(sql))
                    return;
//                if("select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 1, 3, 0)".equals(sql))
//                    System.out.println();
                if (sqlRet.length == 1) {
                    //长度1，non-query语句
                    connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                    if ("merge".equals(sql)) {
//                        Thread.sleep(3000);
                        System.out.println("process merge operation");
                    }
                    statement.close();
                } else {
                    //长度2，query语句，第二项是结果
//                    String[] retArray = (String[]) sqlRet[1];
                    query(sql, sqlRet);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
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
                // System.out.println(hasResultSet + "...");
                //        KvMatchIndexQueryPlan planForHeader = new KvMatchIndexQueryPlan(null, null, 0,0,0);
                if (hasResultSet) {
                    ResultSet resultSet = statement.getResultSet();
                    int cnt = 1;
                    while (resultSet.next()) {
                        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(1)
                                + "," + resultSet.getString(2)
                                + "," + resultSet.getString(3);
                        System.out.println("testtest-actual\t" + ans);
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
                Assert.assertEquals(e.getMessage(), "The timeseries root.vehicle.d0.s0 hasn't been indexed.");
                Assert.assertEquals(querySQL, "select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 6, 9, " +
                        "0.0, 1.0, 0.0) from root.vehicle.d0.s0");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
