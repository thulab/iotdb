package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.service.Daemon;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;

/**
 * @author Jiaye Wu, CGF
 */
public class KvMatchTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KvMatchTest.class);

    private final String FOLDER_HEADER = "src/test/resources";

    private static final String NAMESPACE_PATH = "root.vehicle";
    private static final String CREATE_SERIES_TEMPLATE = "create timeseries " + NAMESPACE_PATH + ".%s.%s with datatype=%s, encoding=%s";
    private static final String SET_STORAGE_GROUP_TEMPLATE = "set storage group to " + NAMESPACE_PATH;
    private static final String CREATE_INDEX_TEMPLATE = "create index on " + NAMESPACE_PATH + ".%s.%s using kv-match";
    private static final String SUB_MATCH_TEMPLATE = "select index subm(" + NAMESPACE_PATH + ".%s.%s, " + NAMESPACE_PATH + ".%s.%s, %s, %s, %s)";
    private static final String DROP_INDEX_TEMPLATE = "drop index on " + NAMESPACE_PATH + ".%s.%s";
    private static final String INSERT_DATA_TEMPLATE = "insert into " + NAMESPACE_PATH + ".%s(timestamp,%s) values (%s,%s)";
    private static final String UPDATE_DATA_TEMPLATE = "update " + NAMESPACE_PATH + ".%s.%s set value=%s where time>=%s and time<=%s";
    private static final String MERGE_TEMPLATE = "merge";
    private static final String CLOSE_TEMPLATE = "close";

    private TsfileDBConfig config;
    private String overflowDataDirPre;
    private String fileNodeDirPre;
    private String bufferWriteDirPre;
    private String metadataDirPre;
    private String derbyHomePre;
    private String indexFileDirPre;
    private String walFolderPre;

    private Daemon daemon;
    private Connection connection = null;

    @Before
    public void setUp() throws Exception {
        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");

        config = TsfileDBDescriptor.getInstance().getConfig();
        overflowDataDirPre = config.overflowDataDir;
        fileNodeDirPre = config.fileNodeDir;
        bufferWriteDirPre = config.bufferWriteDir;
        metadataDirPre = config.metadataDir;
        derbyHomePre = config.derbyHome;
        indexFileDirPre = config.indexFileDir;
        walFolderPre = config.walFolder;

        config.dataDir = FOLDER_HEADER + "/data";
        config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
        config.fileNodeDir = FOLDER_HEADER + "/data/digest";
        config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
        config.metadataDir = FOLDER_HEADER + "/data/metadata";
        config.derbyHome = FOLDER_HEADER + "/data/derby";
        config.indexFileDir = FOLDER_HEADER + "/data/index";
        config.walFolder = FOLDER_HEADER + "/data/wals";
        daemon = new Daemon();
        daemon.active();
    }

    @After
    public void tearDown() throws Exception {
        daemon.stop();
        Thread.sleep(5000);

        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        FileUtils.deleteDirectory(new File(config.overflowDataDir));
        FileUtils.deleteDirectory(new File(config.fileNodeDir));
        FileUtils.deleteDirectory(new File(config.bufferWriteDir));
        FileUtils.deleteDirectory(new File(config.metadataDir));
        FileUtils.deleteDirectory(new File(config.derbyHome));
        FileUtils.deleteDirectory(new File(config.indexFileDir));
        FileUtils.deleteDirectory(new File(config.walFolder));
        FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));

        config.overflowDataDir = overflowDataDirPre;
        config.fileNodeDir = fileNodeDirPre;
        config.bufferWriteDir = bufferWriteDirPre;
        config.metadataDir = metadataDirPre;
        config.derbyHome = derbyHomePre;
        config.indexFileDir = indexFileDirPre;
        config.walFolder = walFolderPre;
    }

    @Test
    public void test() throws ClassNotFoundException, SQLException, InterruptedException {
        Thread.sleep(5000);

        connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

        try {
            createSeries();

            insertData(1, 20000);
            testWithoutOverflow();

            insertData(20001, 1000);
            updateData(9000, 100, 2000);
            updateData(2000, 1000, 1000);
            testWithOverflow();

            testDropIndex();
        } finally {
            if (connection != null)
                connection.close();
        }
    }

    private void createSeries() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(CREATE_SERIES_TEMPLATE, "d0", "s0", TSDataType.INT32, TSEncoding.RLE));
            statement.execute(String.format(CREATE_SERIES_TEMPLATE, "d1", "s1", TSDataType.DOUBLE, TSEncoding.RLE));
            statement.execute(SET_STORAGE_GROUP_TEMPLATE);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertData(int start, int length) throws ClassNotFoundException, SQLException {
        try (Statement statement = connection.createStatement()) {
            int s0Value = 2000;
            double s1Value = 2000;
            for (int i = start; i <= start + length - 1; i++) {
                if (i % 10000 == 0) {
                    LOGGER.info("{}", i);
                }

                if (i >= start + 9000 - 1 && i <= start + 12000 - 1) {
                    statement.execute(String.format(INSERT_DATA_TEMPLATE, "d0", "s0", i, 1000));
                    statement.execute(String.format(INSERT_DATA_TEMPLATE, "d1", "s1", i, 1000.0));
                } else {
                    statement.execute(String.format(INSERT_DATA_TEMPLATE, "d0", "s0", i, s0Value));
                    statement.execute(String.format(INSERT_DATA_TEMPLATE, "d1", "s1", i, s1Value));
                    s0Value += 10;
                    s1Value += 10.0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testWithoutOverflow() throws ClassNotFoundException, SQLException {
        try (Statement statement = connection.createStatement()) {
            // query without index: error
            try {
                statement.execute(String.format(SUB_MATCH_TEMPLATE, "d0", "s0", "d1", "s1", 10000, 11000, 5.0));
            } catch (Exception e) {
                Assert.assertEquals("The timeseries " + NAMESPACE_PATH + ".d0.s0 hasn't been indexed", e.getMessage());
            }

            // create index
            statement.execute(String.format(CREATE_INDEX_TEMPLATE, "d0", "s0"));
            try {
                statement.execute(String.format(CREATE_INDEX_TEMPLATE, "d0", "s0"));
            } catch (Exception e) {
                Assert.assertEquals("The timeseries " + NAMESPACE_PATH + ".d0.s0 has already been indexed.", e.getMessage());
            }

            // query with index: data all in memory
            boolean hasResultSet = statement.execute(String.format(SUB_MATCH_TEMPLATE, "d0", "s0", "d1", "s1", 10000, 10999, 5.0));
            Assert.assertEquals(true, hasResultSet);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    LOGGER.debug("{} {} {} {}", resultSet.getString(0), resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                    cnt++;
                }
                Assert.assertEquals(2002, cnt);
            }

            // close: flush data in buffer write to disk
            statement.execute(CLOSE_TEMPLATE);
            Thread.sleep(5000);

            // query with index: data all on disk
            hasResultSet = statement.execute(String.format(SUB_MATCH_TEMPLATE, "d0", "s0", "d1", "s1", 10000, 10999, 5.0));
            Assert.assertEquals(true, hasResultSet);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    cnt++;
                }
                Assert.assertEquals(2002, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateData(int start, int length, int value) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(UPDATE_DATA_TEMPLATE, "d0", "s0", value, start, start + length - 1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testWithOverflow() {
        try (Statement statement = connection.createStatement()) {
            // query with index and updated interval
            boolean hasResultSet = statement.execute(String.format(SUB_MATCH_TEMPLATE, "d0", "s0", "d1", "s1", 10000, 10999, 5.0));
            Assert.assertEquals(true, hasResultSet);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    LOGGER.debug("{} {} {} {}", resultSet.getString(0), resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
                    cnt++;
                }
                Assert.assertEquals(1903, cnt);
            }

            // close and merge
            statement.execute(CLOSE_TEMPLATE);
            Thread.sleep(5000);
            statement.execute(MERGE_TEMPLATE);
            Thread.sleep(5000);

            // query after merge
            hasResultSet = statement.execute(String.format(SUB_MATCH_TEMPLATE, "d0", "s0", "d1", "s1", 10000, 10999, 5.0));
            Assert.assertEquals(true, hasResultSet);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 0;
                while (resultSet.next()) {
                    cnt++;
                }
                Assert.assertEquals(1903, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testDropIndex() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(DROP_INDEX_TEMPLATE, "d0", "s0"));
            statement.close();

            File indexFileDir = new File(config.indexFileDir);
            File[] files = new File[1];
            files[0] = new File(config.indexFileDir + File.separator + ".metadata");
            Assert.assertArrayEquals(files, indexFileDir.listFiles());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
