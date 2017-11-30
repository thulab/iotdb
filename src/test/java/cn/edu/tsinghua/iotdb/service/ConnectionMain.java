package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

import java.sql.*;

import static cn.edu.tsinghua.iotdb.service.TestUtils.count;
import static cn.edu.tsinghua.iotdb.service.TestUtils.max_value;
import static org.junit.Assert.fail;

/**
 * test class
 * connection to remote server
 */
public class ConnectionMain {

    private static final String TIMESTAMP_STR = "Time";
    private static final String d_56 = "root.performf.group_5.d_56.s_94";
    private static final String d_66 = "root.performf.group_6.d_66.s_10";

    static String sql1 = "SELECT max_value(s_94) FROM root.performf.group_5.d_56 GROUP BY(250000ms, 1262275200000,[1262275200000,1262276200000])";
    static String sql2 = "SELECT max_value(s_10) FROM root.performf.group_6.d_66 GROUP BY(250000ms, 1262275200000,[1262425699500,1262426699500])";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.9:6667/", "root", "root");
            Statement statement = connection.createStatement();

            System.out.println("test 1 ======= ");
            boolean hasResultSet = statement.execute(sql1);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d_56));
                    System.out.println(ans);
                    cnt ++;
                }
                System.out.println(cnt);
            }
            statement.close();

            System.out.println("test 2 ======= ");
            statement = connection.createStatement();
            hasResultSet = statement.execute(sql2);
            if (hasResultSet) {
                ResultSet resultSet = statement.getResultSet();
                int cnt = 1;
                while (resultSet.next()) {
                    String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d_66));
                    System.out.println(ans);
                    cnt ++;
                }
                System.out.println(cnt);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (connection != null) {
                connection.close();
            }
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}

