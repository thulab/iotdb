package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

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
			statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt01");
			statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
			statement.execute("CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
//			statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)");
//			statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)");
//			statement.execute("insert into root.ln.wf01.wt01(timestamp,status) values(1509465720000,false)");
//			statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
//			statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)");
//			statement.execute("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465720000,20.092794)");
//			ResultSet resultSet = statement.executeQuery("select * from root");
//			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//			while(resultSet.next()){
//				StringBuilder  builder = new StringBuilder();
//				for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
//					builder.append(resultSet.getString(i)).append(",");
//				}
//				System.out.println(builder);
//			}
			statement.close();
			
			PreparedStatement preparedStatement = connection.prepareStatement("insert into root.ln.wf01.wt01(timestamp,status,temperature) values(?,?,?)");
			preparedStatement.setLong(1, 1509465600000L);
			preparedStatement.setBoolean(2, true);
			preparedStatement.setFloat(3, 25.957603f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
			
			preparedStatement.setLong(1, 1509465660000L);
			preparedStatement.setBoolean(2, true);
			preparedStatement.setFloat(3, 24.359503f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
			
			preparedStatement.setLong(1, 1509465720000L);
			preparedStatement.setBoolean(2, false);
			preparedStatement.setFloat(3, 20.092794f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
			
			
			preparedStatement.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:03:00"));
			preparedStatement.setBoolean(2, false);
			preparedStatement.setFloat(3, 20.092794f);
			preparedStatement.execute();
			preparedStatement.clearParameters();
						
			preparedStatement.close();
			
			ResultSet resultSet = preparedStatement.executeQuery("select * from root");
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while(resultSet.next()){
				StringBuilder  builder = new StringBuilder();
				for (int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
					builder.append(resultSet.getString(i)).append(",");
				}
				System.out.println(builder);
			}
			preparedStatement.close();

			
//			for(int i = 1; i <= resultSetMetaData.getColumnCount();i++) {
//				System.out.println(resultSetMetaData.getColumnType(i)+"-"+resultSetMetaData.getColumnName(i));
//			}
		} finally {
			connection.close();
		}
	}

}
