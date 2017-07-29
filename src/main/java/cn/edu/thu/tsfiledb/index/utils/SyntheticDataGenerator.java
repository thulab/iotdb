package cn.edu.thu.tsfiledb.index.utils;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The class generate synthetic data series to TsFileDB for index building.
 *
 * @author Jiaye Wu
 */
public class SyntheticDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SyntheticDataGenerator.class);

    private static final String CREATE_TIME_SERIES_TEMPLATE = "create timeseries root.laptop.%s.%s with datatype=%s,encoding=%s";
    private static final String INSERT_DATA_TEMPLATE = "insert into root.laptop.%s(timestamp,%s) values (%s,%s)";
    private static final String SET_STORAGE_GROUP_TEMPLATE = "set storage group to root.laptop.%s";

    private static final String JDBC_SERVER_URL = "jdbc:tsfile://127.0.0.1:6667/";
//	private static final String JDBC_SERVER_URL = "jdbc:tsfile://192.168.130.15:6667/";

    private Connection connection = null;

    private String deviceName;
    private int length;
    private long timeInterval;

    public SyntheticDataGenerator(String deviceName, int length, long timeInterval) {
        this.deviceName = deviceName;
        this.length = length;
        this.timeInterval = timeInterval;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        long time = System.currentTimeMillis();
        SyntheticDataGenerator generator1 = new SyntheticDataGenerator("d1", 10000000, 10);
        generator1.start(time);
//		SyntheticDataGenerator generator2 = new SyntheticDataGenerator("d2", 1000000, 100);
//		generator2.start(time);
    }

    public void start(long t) throws ClassNotFoundException, SQLException {
        Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
        connectServer();

        createTimeSeriesMetadata();

        Statement statement = connection.createStatement();
        int x1 = ThreadLocalRandom.current().nextInt(-5, 5);
        for (int i = 0; i < length; i++) {
            if (i % 10000 == 0) {
                logger.info("{}", i);
            }

            statement.execute(String.format(INSERT_DATA_TEMPLATE, deviceName, "s1", t, x1));

            x1 += ThreadLocalRandom.current().nextInt(-1, 1);
            t += timeInterval;
        }

        disconnectServer();
    }

    private void createTimeSeriesMetadata() throws SQLException {
        List<String> sqls = new ArrayList<>();
        sqls.add(String.format(CREATE_TIME_SERIES_TEMPLATE, deviceName, "s1", TSDataType.INT32, TSEncoding.RLE));
        sqls.add(String.format(CREATE_TIME_SERIES_TEMPLATE, deviceName, "s2", TSDataType.INT64, TSEncoding.RLE));
        sqls.add(String.format(SET_STORAGE_GROUP_TEMPLATE, deviceName));
        executeSQL(sqls);
    }

    private void insertData(long t, int s1, long s2) throws SQLException {
        List<String> sqls = new ArrayList<>();
        sqls.add(String.format(INSERT_DATA_TEMPLATE, deviceName, "s1", t, s1));
        sqls.add(String.format(INSERT_DATA_TEMPLATE, deviceName, "s2", t, s2));
        executeSQL(sqls);
    }

    private void connectServer() {
        try {
            connection = DriverManager.getConnection(JDBC_SERVER_URL, "root", "root");
        } catch (SQLException e) {
            logger.error("Failed to connect the server {} because ", JDBC_SERVER_URL, e);
            System.exit(1);
        }
    }

    private void disconnectServer() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Failed to disconnect the server {} because ", JDBC_SERVER_URL, e);
            }
        }
    }

    private void executeSQL(List<String> sqls) throws SQLException {
        if (connection == null) {
            connectServer();
        }
        try {
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                try {
                    statement.execute(sql);
                } catch (Exception e) {
                    logger.error("Execute {} failed!", sql, e);
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to execute {} because ", sqls, e);
        }
    }
}
