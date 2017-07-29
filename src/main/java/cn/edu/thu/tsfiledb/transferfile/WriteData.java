package cn.edu.thu.tsfiledb.transferfile;

import java.sql.*;

/**
 * Created by lylw on 2017/7/29.
 */
public class WriteData {
    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");

            statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select * from root.vehicle");
            if (hasResultSet) {
                System.out.println("hasResultSet");
                ResultSet res = statement.getResultSet();
                while (res.next()) {
                    System.out.println("In res.next()");
                    System.out.println( res.getInt("s0"));
                }
            }
        } catch (Exception e) {
        } finally {
            if(statement != null){
                statement.close();
            }
            if(connection != null){
                connection.close();
            }
        }
    }
}
