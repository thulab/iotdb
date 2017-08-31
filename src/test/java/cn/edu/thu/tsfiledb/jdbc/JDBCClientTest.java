package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCClientTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		try {
			connection =  DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
			DatabaseMetaData databaseMetaData = connection.getMetaData();

			
			ResultSet resultSet = databaseMetaData.getColumns(null, null, "root.*", null);
			while(resultSet.next()){
				System.out.println(String.format("column %s", resultSet.getString(0)));
			}
		} finally {
			connection.close();
		}
	}

}
