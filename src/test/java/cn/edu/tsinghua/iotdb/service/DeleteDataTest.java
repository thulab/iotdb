package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeleteDataTest {

    private static final String STORAGE_GROUP = "root.d0";
    private static final String INSERT_TEMPLATE = "INSERT INTO root.d0(timestamp,s0,s1) VALUES (%d,%d,%f)";
    private static final String[] s = {"s0", "s1"};
    private static final String[] CREATE_TEMPLATES = {"SET STORAGE GROUP TO root.d0",
                                                    "CREATE TIMESERIES root.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN",
                                                    "CREATE TIMESERIES root.d0.s1 WITH DATATYPE=DOUBLE,ENCODING=PLAIN",};
    private static final String QUERY_TEMPLATE = "SELECT s0,s1 from root.d0";
    private static final String DELETE_TEMPLATE = "DELETE FROM root.d0.* where time <= %s";

    private IoTDB deamon;
    private Connection connection;

    private boolean testFlag = TestUtils.testFlag;


    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();

            Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            if (connection != null) {
                connection.close();
            }
            deamon.stop();
            Thread.sleep(1000);
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void inMemTest() throws SQLException {
        // this test deletes unflushed data in memory
        // prepare data
        int dataSize = 500, offset = 0;
        prepareData(dataSize, offset);
        // delete half of the data
        exeDelete(250);
        // query and check
        Map<String, List<Object>> result = query();
        List<Object> s0Data = result.get(s[0]);
        List<Object> s1Data = result.get(s[1]);
        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
        for (int i = 0; i < 250; i++) {
            int s0 = (int) s0Data.get(i);
            double s1 = (double) s1Data.get(i);
            assertEquals(i + 251, s0);
            assertEquals((i + 251) * 1.0, s1, 0.000000001);
        }
    }

    @Test
    public void inFileTest() throws SQLException {
        // this test deletes flushed data in the file
        // prepare data
        int dataSize = 500, offset = 0;
        prepareData(dataSize, offset);
        flush();
        // delete half of the data
        int deleteTime = 250;
        exeDelete(deleteTime);
        // query and check
        Map<String, List<Object>> result = query();
        List<Object> s0Data = result.get(s[0]);
        List<Object> s1Data = result.get(s[1]);
        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
        for (int i = 0; i < 250; i++) {
            int s0 = (int) s0Data.get(i);
            double s1 = (double) s1Data.get(i);
            assertEquals(i + deleteTime + 1, s0);
            assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
        }
    }

    @Test
    public void inMemAndFileTest() throws SQLException {
        // this test deletes both in memory and in file
        // prepare data in file
        int dataSize = 500, offset = 0;
        prepareData(dataSize, offset);
        flush();
        // prepare data in memory
        offset = 500;
        prepareData(dataSize, offset);
        // delete half of the data in memory and all data in file
        int deleteTime = 750;
        exeDelete(deleteTime);
        // query and check
        Map<String, List<Object>> result = query();
        List<Object> s0Data = result.get(s[0]);
        List<Object> s1Data = result.get(s[1]);
        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
        for (int i = 0; i < 250; i++) {
            int s0 = (int) s0Data.get(i);
            double s1 = (double) s1Data.get(i);
            assertEquals(i + deleteTime + 1, s0);
            assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
        }
    }

    @Test
    public void deleteOverflow() throws SQLException {
        // this test deletes data in both overflow data and normal tsfile data
        // prepare data in tsfile
        int dataSize = 500, offset = 500;
        prepareData(dataSize, offset);
        flush();
        // prepare data in overflow
        offset = 0;
        prepareData(dataSize, offset);
        flush();
        // delete half of the data in tsfile and all data in overflow
        int deleteTime = 750;
        exeDelete(deleteTime);
        // query and check
        Map<String, List<Object>> result = query();
//        List<Object> s0Data = result.get(s[0]);
//        List<Object> s1Data = result.get(s[1]);
//        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
//        for (int i = 0; i < 250; i++) {
//            int s0 = (int) s0Data.get(i);
//            double s1 = (double) s1Data.get(i);
//            assertEquals(i + deleteTime + 1, s0);
//            assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
//        }
    }

    @Test
    public void multipleTombstoneTest() throws SQLException {
        // this test perform multiple deletion on the same series
        // prepare data in tsfile
        int dataSize = 500, offset = 500;
        prepareData(dataSize, offset);
        flush();
        exeDelete(200);
        // prepare data in overflow
        offset = 0;
        prepareData(dataSize, offset);
        flush();
        exeDelete(500);
        // delete half of the data in tsfile and all data in overflow
        int deleteTime = 750;
        exeDelete(deleteTime);

        // query and check
        Map<String, List<Object>> result = query();
        List<Object> s0Data = result.get(s[0]);
        List<Object> s1Data = result.get(s[1]);
        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
        for (int i = 0; i < 250; i++) {
            int s0 = (int) s0Data.get(i);
            double s1 = (double) s1Data.get(i);
            assertEquals(i + deleteTime + 1, s0);
            assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
        }
    }

    @Test
    public void insertAfterDeleteTest() throws SQLException {
        // this test inserts data after deletion to ensure later data are not affected
        // prepare data in file
        int dataSize = 500, offset = 0;
        prepareData(dataSize, offset);
        flush();
        // prepare data in memory
        offset = 500;
        prepareData(dataSize, offset);
        // delete half of the data in memory and all data in file
        exeDelete(900);
        prepareData(150, 750);
        int deleteTime = 750;
        // query and check
        Map<String, List<Object>> result = query();
        List<Object> s0Data = result.get(s[0]);
        List<Object> s1Data = result.get(s[1]);
        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
        for (int i = 0; i < 250; i++) {
            int s0 = (int) s0Data.get(i);
            double s1 = (double) s1Data.get(i);
            assertEquals(i + deleteTime + 1, s0);
            assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
        }
    }


    @Test
    public void overflowMergeTombstoneTest() throws SQLException, InterruptedException {
        long threshold = TsfileDBDescriptor.getInstance().getConfig().overflowFileSizeThreshold;
        TsfileDBDescriptor.getInstance().getConfig().overflowFileSizeThreshold = 1;
        try {
            // this test deletes data in both overflow data and normal tsfile data
            // then calls merge to clean tombstone file
            // prepare data in tsfile
            int dataSize = 1000, offset = 0;
            prepareData(dataSize, offset);
            flush();
            // prepare data in overflow
            offset = 500;
            dataSize = 500;
            prepareData(dataSize, offset);
            flush();
            // delete half of the data in tsfile and all data in overflow
            int deleteTime = 750;
            exeDelete(deleteTime);
            // check that tombstone file exists
            File storageGroupDir = new File(TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs()[0] + File.separator + STORAGE_GROUP);
            File[] files = storageGroupDir.listFiles();
            assertTrue(files != null);
            boolean exists = false;
            for(File file : files)
                if(file.getName().contains("tombstone") && file.length() > 0)
                    exists = true;
            assertTrue(exists);

            merge();
            Thread.sleep(10000);

            files = storageGroupDir.listFiles();
            assertTrue(files != null);
            exists = false;
            for(File file : files)
                if(file.getName().contains("tombstone") && file.length() > 0)
                    exists = true;
            assertTrue(!exists);

            // query and check
            Map<String, List<Object>> result = query();
            List<Object> s0Data = result.get(s[0]);
            List<Object> s1Data = result.get(s[1]);
            assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 250);
            for (int i = 0; i < 250; i++) {
                int s0 = (int) s0Data.get(i);
                double s1 = (double) s1Data.get(i);
                assertEquals(i + deleteTime + 1, s0);
                assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
            }
        } finally {
            TsfileDBDescriptor.getInstance().getConfig().overflowFileSizeThreshold = threshold;
        }
    }

    @Test
    public void tombstoneMergeTest() throws SQLException, InterruptedException {
        // this test delete data and wait until the tombstone is merged
        // prepare data
        int dataSize = 1000, offset = 0;
        prepareData(dataSize, offset);
        flush();
        // delete half of the data in tsfile
        int deleteTime = 500;
        exeDelete(deleteTime);
        // check that tombstone file exists
        File storageGroupDir = new File(TsfileDBDescriptor.getInstance().getConfig().getBufferWriteDirs()[0] + File.separator + STORAGE_GROUP);
        File[] files = storageGroupDir.listFiles();
        assertTrue(files != null);
        boolean exists = false;
        for(File file : files)
            if(file.getName().contains("tombstone") && file.length() > 0)
                exists = true;
        assertTrue(exists);

        merge();
        Thread.sleep(10000);

        files = storageGroupDir.listFiles();
        assertTrue(files != null);
        exists = false;
        for(File file : files)
            if(file.getName().contains("tombstone") && file.length() > 0)
                exists = true;
        assertTrue(!exists);

        // query and check
        Map<String, List<Object>> result = query();
        List<Object> s0Data = result.get(s[0]);
        List<Object> s1Data = result.get(s[1]);
        assertTrue(s0Data.size() == s1Data.size() && s1Data.size() == 500);
        for (int i = 0; i < 500; i++) {
            int s0 = (int) s0Data.get(i);
            double s1 = (double) s1Data.get(i);
            assertEquals(i + deleteTime + 1, s0);
            assertEquals((i + deleteTime + 1) * 1.0, s1, 0.000000001);
        }
    }

    private void merge() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("merge");
        statement.close();
    }

    private void flush() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("flush");
        statement.close();
    }

    private Map<String, List<Object>> query() throws SQLException {
        Map<String, List<Object>> ret = new HashMap<>();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(QUERY_TEMPLATE);
        List<Object> s0Data = new ArrayList<>();
        List<Object> s1Data = new ArrayList<>();
        while (resultSet.next()) {
            s0Data.add(resultSet.getInt(1));
            s1Data.add(resultSet.getDouble(2));
        }
        resultSet.close();
        statement.close();
        ret.put(s[0], s0Data);
        ret.put(s[1], s1Data);
        return ret;
    }

    private void exeDelete(long time) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = String.format(DELETE_TEMPLATE, time);
        statement.execute(sql);
        statement.close();
    }

    private void prepareSeries() throws SQLException {
        Statement statement = connection.createStatement();
        // create storage group and time series
        for (String sql : CREATE_TEMPLATES) {
            statement.execute(sql);
        }
        statement.close();
    }

    private void insertData(long[] timestamps, int[] s0Data, double[] s1Data, int batchSize) throws SQLException {
        Statement statement = connection.createStatement();
        int i = 0;
        for(; i < timestamps.length; i++) {
            String sql = String.format(INSERT_TEMPLATE, timestamps[i], s0Data[i], s1Data[i]);
            if(batchSize > 1) {
                statement.addBatch(sql);
                if((i + 1) % batchSize == 0) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
            } else {
                statement.execute(sql);
            }
        }
        if(batchSize > 1 && (i % batchSize) != 0) {
            statement.executeBatch();
        }
        statement.close();
    }

    private void prepareData(int dataSize, int offset) throws SQLException {
        try {
            prepareSeries();
        } catch (SQLException ignored) {
        }
        // define data
        long[] timestamps = new long[dataSize];
        int[] s0Data = new int[dataSize];
        double[] s1Data = new double[dataSize];
        for (int i = 0; i < dataSize; i++) {
            timestamps[i] = (i + offset) + 1;
            s0Data[i] = (i + offset) + 1;
            s1Data[i] = (i + offset) * 1.0;
        }
        // insert data
        insertData(timestamps, s0Data, s1Data, dataSize);
    }

}
