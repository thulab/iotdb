package cn.edu.tsinghua.iotdb.performance.index;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.jdbc.TsfileSQLException;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class KvIndexPerfTestInstance {
    private static IoTDB deamon;
    //We insert all data into one path and create index.
    private static String path;
    //Another path to cut off testing path
    private static int defaultWindowLength;
    private static long defaultPatternStartPos;
    private static long defaultPatternLength;
    private static float defaultThreshold = 0.2f;

    private static final String FOLDER_HEADER = "src/test/resources";
    private static int maxOpenFolderPre;
    private static String overflowDataDirPre;
    private static String fileNodeDirPre;
    private static String bufferWriteDirPre;
    private static String metadataDirPre;
    private static String derbyHomePre;
    private static String walFolderPre;
    private static String indexFileDirPre;

    //TODO to be specified, the last timestamp of this series
    private static long[] lastTimestamp;

    private static long defaultLimitTime = 3000L;

    private static String resultFile;

    private static FileWriter resultWriter;

    public static void setUp() throws Exception {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
//        clearDir(config);
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

        resultFile = "result.out";

        deamon = new IoTDB();
        deamon.active();

        File ff = new File(config.bufferWriteDir);
        prepareIoTData(ff.exists());
    }

    private static void clearDir(TsfileDBConfig config) throws IOException {
        FileUtils.deleteDirectory(new File(config.overflowDataDir));
        FileUtils.deleteDirectory(new File(config.fileNodeDir));
        FileUtils.deleteDirectory(new File(config.bufferWriteDir));
        FileUtils.deleteDirectory(new File(config.metadataDir));
        FileUtils.deleteDirectory(new File(config.derbyHome));
        FileUtils.deleteDirectory(new File(config.walFolder));
        FileUtils.deleteDirectory(new File(config.indexFileDir));
        FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));
    }

    private static void prepareIoTData(Boolean exists) throws IOException, InterruptedException, WriteProcessException, ClassNotFoundException, SQLException{
        String[] sqls = new String[]{
                "SET STORAGE GROUP TO root.vehicle.d40",
                "CREATE TIMESERIES root.vehicle.d40.s5 WITH DATATYPE=INT32, ENCODING=RLE",
        };
        defaultWindowLength = 50;
        defaultPatternStartPos = 1;
        lastTimestamp = new long[]{1000000, 10000000, 100000000, 1000000000, 2000000000, 3000000000l};
        defaultPatternLength = 50;

        int pos = 1;
        double[][] mul = new double[][]{
                {0, 0.33, 1.0}, {0, 0.167, 0.5, 1.0}, {0, 0.1, 0.3, 0.6, 1.0}
        };

        if (exists) return;
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }
            for (int i = 0;i < 4 * 7;i++) {
                int j = i % 7;
                int k = i / 7;
                String sql;
                sql = String.format("SET STORAGE GROUP TO root.vehicle.d%d", i);
                statement.execute(sql);
                sql = String.format("CREATE TIMESERIES root.vehicle.d%d.s5 WITH DATATYPE=INT32, ENCODING=RLE", i);
                statement.execute(sql);
                if (j == 0) {
                    sql = String.format("insert into root.vehicle.d%d(timestamp,s5) values(1,1)", i);
                    statement.execute(sql);
                    sql = String.format("insert into root.vehicle.d%d(timestamp,s5) values(%d, %d)", i, lastTimestamp[k], lastTimestamp[k]);
                    statement.execute(sql);
                    sql = String.format("insert into root.vehicle.d40(timestamp,s5) values(%d,%d)", pos, pos);
                    statement.execute(sql);
                    pos++;
                } else if (j < 4) {
                    for (int n = 0;n < (j+1);n++) {
                        sql = String.format("insert into root.vehicle.d%d(timestamp,s5) values(%d,%d)", i, n * lastTimestamp[k] / (j+1) + 1, n * lastTimestamp[k] / (j+1) + 1);
                        statement.execute(sql);
                        sql = String.format("insert into root.vehicle.d%d(timestamp,s5) values(%d,%d)", i, (n + 1) * lastTimestamp[k] / (j+1), (n + 1) * lastTimestamp[k] / (j+1));
                        statement.execute(sql);
                        sql = String.format("insert into root.vehicle.d40(timestamp,s5) values(%d,%d)", pos, pos);
                        statement.execute(sql);
                        pos++;
                    }
                } else {
                    for (int n = 0;n < (j-2);n++) {
                        sql = String.format("insert into root.vehicle.d%d(timestamp,s5) values(%d,%d)", i, (long)(lastTimestamp[k] * mul[j-4][n]) + 1, (long)(lastTimestamp[k] * mul[j-4][n]) + 1);
                        statement.execute(sql);
                        sql = String.format("insert into root.vehicle.d%d(timestamp,s5) values(%d,%d)", i, (long)(lastTimestamp[k] * mul[j-4][n+1]), (long)(lastTimestamp[k] * mul[j-4][n+1]));
                        statement.execute(sql);
                        sql = String.format("insert into root.vehicle.d40(timestamp,s5) values(%d,%d)", pos, pos);
                        statement.execute(sql);
                        pos++;
                    }
                }
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        String[][] sensors = new String[][]{
                {"s5"}, {"s5","s0"},{"s5","s0","s1"}, {"s5","s0","s1","s2"}
        };
        for (int i = 0;i < 4 * 7;i++) {
            int j = i % 7;
            int k = i / 7;
            if (k != 0 && j != 0) continue;
            File dir = new File(config.bufferWriteDir + "/root.vehicle.d" + i);
            File[] files = dir.listFiles();
            if (files.length == 0)
                continue;
            if (j == 0) {
                String filename = files[0].getAbsolutePath();
                files[0].delete();
                GenBigTsFile.generate(lastTimestamp[k], filename, sensors[0], i, 1);
            }
        }
    }

    public static void tearDown() throws Exception {
        deamon.stop();
        Thread.sleep(5000);

        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
//        clearDir(config);
        config.overflowDataDir = overflowDataDirPre;
        config.fileNodeDir = fileNodeDirPre;
        config.bufferWriteDir = bufferWriteDirPre;
        config.metadataDir = metadataDirPre;
        config.derbyHome = derbyHomePre;
        config.maxOpenFolder = maxOpenFolderPre;
        config.walFolder = walFolderPre;
        config.indexFileDir = indexFileDirPre;
    }

    public static void Test() throws IOException, SQLException, ClassNotFoundException {
        createPerfTest();
//        queryPerfByVaryTimeRangeTest();
//        queryPerfByVaryThresholdTest();
//        queryPerfByVaryPatternLengthTest();
//        queryPerfByVaryWindowSizeTest();
//        executeSQL("drop index kvindex on " + path, 0);
    }

    //    @Test
    public static void createPerfTest() throws IOException, SQLException, ClassNotFoundException {
        System.out.println("create time cost");
        //suppose the time range of the path is 0~x, we test the time costs of creating index over 10%, 20%, ...
        //90% of the whole time range.
        for (int i = 0; i < 4;i++) {

            path = "root.vehicle.d" + (i * 7) + " .s5";

            try {
                executeSQL("drop index kvindex on " + path, 0);
            } catch (Exception e) {
            }

            double averageTime = executeSQL("create index on " + path + " using kvindex with window_length=" +
                    defaultWindowLength + ", " + "since_time=" + 0, 0);

            System.out.println("percent: " + lastTimestamp[i] + "\ttime:" + averageTime);

            double aT = query(String.format("select index kvindex(%s, %s, %s, %s, %s, 1.0, 0.0) from %s",
                    path, path, defaultPatternStartPos, (defaultPatternStartPos + defaultPatternLength - 1), defaultThreshold,
                    path), defaultLimitTime);

            resultWriter = new FileWriter(resultFile, true);
            resultWriter.write("length_test:\tsingle file:\tlength: " + lastTimestamp[i] + "\ttime: " + averageTime + "s\tquery time: " + aT + "s\n");
            resultWriter.close();
        }
        //finally, create index over the whole time series for the following query test.
//        executeSQL("create index on " + path + " using kvindex with window_length=" +
//                defaultWindowLength + ", " + "since_time=0", 0);
    }

    //    @Test
    public static void queryPerfByVaryPatternLengthTest() throws IOException, SQLException, ClassNotFoundException {
        System.out.println("query by varying pattern length");
        //suppose the time range of the path is 0~x, we test the time costs of creating index over 10%, 20%, ...
        //90% of the whole time range.
//        for (float i = 0.2f; i < 2f; i += 0.2) {
        path = "root.vehicle.d14.s5";
        for (int i = 1; i <= 5;i++){
            int patternLength = (int) (defaultPatternLength * Math.pow(10, i));

            double averageTime = query(String.format("select index kvindex(%s, %s, %s, %s, %s, 1.0, 0.0) from %s",
                    path, path, defaultPatternStartPos, (defaultPatternStartPos + patternLength - 1), defaultThreshold,
                    path), defaultLimitTime);

            System.out.println("the ratio of pattern length: " + patternLength + "\ttime:" + averageTime);

            resultWriter = new FileWriter(resultFile, true);
            resultWriter.write("pattern_test:\tsingle file\tlength: " + lastTimestamp[2] + "\tpattern length: " + patternLength + "\ttime: " + averageTime + "s\n");
            resultWriter.close();
        }
    }

//    @Test
/*    public void queryPerfByVaryThresholdTest() throws SQLException, ClassNotFoundException {
        System.out.println("query by varying threshold, baseline: " + defaultThreshold);
        //suppose the time range of the path is 0~x, we test the time costs of creating index over 10%, 20%, ...
        //90% of the whole time range.
        for (float i = 0.f; i < 2f; i += 0.2) {
            float threshold = defaultThreshold * i;

            double averageTime = query(String.format("select index kvindex(%s, %s, %s, %s, %s, 1.0, 0.0) from %s",
                    path, path, defaultPatternStartPos, (defaultPatternStartPos + defaultPatternLength), threshold,
                    path), defaultLimitTime);

            System.out.println("the ratio of pattern threshold: " + i + "\ttime:" + averageTime);
        }
    }*/

//    @Test
/*    public void queryPerfByVaryTimeRangeTest() throws SQLException, ClassNotFoundException {
        System.out.println("query by varying time range");
        //suppose the time range of the path is 0~x, we test the time costs of creating index over 10%, 20%, ...
        //90% of the whole time range.
        for (float i = 0.2f; i <= 1; i += 0.2f) {
            double averageTime = query(String.format("select index kvindex(%s, %s, %s, %s, %s, 1.0, 0.0) from %s where time < %s",
                    path, path, defaultPatternStartPos, (defaultPatternStartPos + defaultPatternLength),
                    defaultThreshold,
                    path, (long) (lastTimestamp[0] * i)), defaultLimitTime);

            System.out.println("the ratio of time range: " + i + "\ttime:" + averageTime);
        }
    }*/

    /**
     *
     */
//    @Test
    public static void queryPerfByVaryWindowSizeTest() throws IOException, SQLException, ClassNotFoundException {
        System.out.println("query by varying window length, baseline: " + defaultWindowLength);
        path = "root.vehicle.d14.s5";
        for (float i = 0.2f; i < 1.0; i += 0.2f) {
            try {
                executeSQL("drop index kvindex on " + path, 0);
            } catch (Exception e) {
            }

            double aT = executeSQL("create index on " + path + " using kvindex with window_length=" +
                    (int) (defaultWindowLength * i) + ", " + "since_time=0", 0);

            System.out.println("the ratio of window length: " + i + "\tindex time:" + aT);

            double averageTime = query(String.format("select index kvindex(%s, %s, %s, %s, %s, 1.0, 0.0) from %s where time < %s",
                    path, path, defaultPatternStartPos, (defaultPatternStartPos + defaultPatternLength - 1),
                    defaultThreshold, path, lastTimestamp[2]),
                    defaultLimitTime);
            System.out.println("the ratio of window length: " + i + "\ttime:" + averageTime);

            resultWriter = new FileWriter(resultFile, true);
            resultWriter.write("window_length_test:\tsingle file\tlength: " + lastTimestamp[2] + "\twindow_length: " + (int)(defaultWindowLength * i) + "\tcreate time: " + aT + "s\tquery time:" + averageTime + "s\n");
            resultWriter.close();
        }
    }

    /**
     * @param sql
     * @param limitTime
     * @return total cycle when this function runs over limitTime
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static double executeSQL(String sql, long limitTime) throws ClassNotFoundException, SQLException {
        long startTime = System.currentTimeMillis();
        long endTime = startTime;
        int cycle = 0;
        while (endTime - startTime <= limitTime) {
            Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
            Connection connection = null;
            try {
                System.out.println("testtest-sql\t" + sql);
                //长度1，non-query语句
                connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
                Statement statement = connection.createStatement();
                statement.execute(sql);
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
//                System.exit(0);
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            endTime = System.currentTimeMillis();
            cycle++;
        }
        return ((double) (endTime - startTime)) / 1000.0 / cycle;
    }

    private static double query(String querySQL, long limitTime) throws ClassNotFoundException,
            SQLException {
        long startTime = System.currentTimeMillis();
        long endTime = startTime;
        int cycle = 0;
        while (endTime - startTime <= limitTime) {
            Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
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
                        while (resultSet.next()) {
                            //don't check anything, just for performance evaluation
                        }
                    }
                } catch (TsfileSQLException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            endTime = System.currentTimeMillis();
            cycle++;
        }
        return ((double) (endTime - startTime)) / 1000 / cycle;
    }

    public static void main(String[] args) throws Exception{
        setUp();
        Test();
        tearDown();
        return;
    }
}
