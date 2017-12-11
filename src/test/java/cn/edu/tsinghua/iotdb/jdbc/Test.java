package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Test {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		Connection connection = null;
		try {
			connection =  DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
//			DatabaseMetaData databaseMetaData = connection.getMetaData();
//			ResultSet resultSet = databaseMetaData.getColumns(null, null, "root.dt.wf02.type1.d3.s1", null);
//			while(resultSet.next()){
//				System.out.println(String.format("column %s, type %s", resultSet.getString("COLUMN_NAME"), resultSet.getString("COLUMN_TYPE")));
//				System.out.println(String.format("column %s, type %s", resultSet.getString(1), resultSet.getString(2)));
//			}
			
//			ResultSet resultSet = databaseMetaData.getColumns(null, null, "root.*", null);
//			while(resultSet.next()){
//				//System.out.println(String.format("column %s, type %s", resultSet.getString("COLUMN_NAME"), resultSet.getString("COLUMN_TYPE")));
//				System.out.println(String.format("column %s", resultSet.getString(1)));
//				System.out.println(String.format("column %s", resultSet.getString("DELTA_OBJECT")));
//			}
			
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("select * from root");
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while(resultSet.next()){
				StringBuilder  builder = new StringBuilder();
				for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
					builder.append(resultSet.getString(i)).append(",");
				}
				System.out.println(builder);
			}
			statement.close();
			
			resultSet = statement.executeQuery("select * from root");
			resultSetMetaData = resultSet.getMetaData();
			while(resultSet.next()){
				StringBuilder  builder = new StringBuilder();
				for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
					builder.append(resultSet.getString(i)).append(",");
				}
				System.out.println(builder);
			}
			statement.close();
			
//			for(int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
//				System.out.println(resultSetMetaData.getColumnType(i)+"-"+resultSetMetaData.getColumnName(i));
//			}
		} finally {
			connection.close();
		}
	}

}
