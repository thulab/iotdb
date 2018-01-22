package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class BatchTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		Connection connection = null;
		try {
			connection =  DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
			Statement statement = connection.createStatement();
			statement.addBatch("SET STORAGE GROUP TO root.ln.wf01.wt01");
			statement.addBatch("CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
			statement.addBatch("CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
			statement.addBatch("insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)");
			statement.addBatch("insert into root.ln.wf01.wt01(timestamp,status) values(1509465660000,true)");
			statement.addBatch("insert into root.ln.wf01.wt01(timestamp,status) 1values(1509465720000,false)");
			statement.addBatch("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465600000,25.957603)");
			statement.addBatch("insert into root.ln.wf01.wt01(timestamp,temperature) values(1509465660000,24.359503)");
			statement.addBatch("insert into root.ln.wf01.wt01(timestamp,temperature) 1values(1509465720000,20.092794)");
			try {
				int[] result = statement.executeBatch();
			} catch (BatchUpdateException e) {
				System.out.println(e.getMessage());
				long[] arr = e.getLargeUpdateCounts();
				for(long i : arr){
					System.out.println(i);
				}
			}
			statement.clearBatch();
			statement.close();
			
		} finally {
			connection.close();
		}
	}

}
