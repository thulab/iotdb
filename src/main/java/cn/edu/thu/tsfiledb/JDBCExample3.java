package cn.edu.thu.tsfiledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCExample3 {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCExample3.class);
	private static String schemaFilePath = "src/main/resources/schema.csv";
	private static String sqlFile = "src/main/resources/sqlfile";
	private static String sourceFilePath = "D:\\datayingyeda";
	private static String host = "127.0.0.1";
	private static String port = "6667";
	private static String username = "root";
	private static String password = "root";
	private static Set<String> set = new HashSet<>();
	private static long count = 0;
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static long startTime = 0;
	private static long endTime = 0;
	private static long wstarttime = 0;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		Connection connection = null;
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		createSchema(schemaFilePath, connection);
		insertData(sqlFile);
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		Statement statement = connection.createStatement();
		System.out.println(statement.execute("close"));
		connection.close();
	}

	private static void mergeData(Connection connection) throws SQLException {

		String mergeSql = "merge";
		Statement statement = connection.createStatement();
		statement.execute(mergeSql);
		statement.close();
	}

	private static String timeTrans(String time) {

		try {
			return String.valueOf(dateFormat.parse(time).getTime());
		} catch (ParseException e) {
			LOGGER.error(e.getMessage());
		}
		return time;
	}

	private static void insertOneData(String line, Statement statement) {

		
		try {
			statement.addBatch(line);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage());
		}
		count++;
		if(count%100==0){
			try {
				statement.executeBatch();
				statement.clearBatch();
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
			}
		}
		if (count % 10000 == 0) {
			wstarttime = System.currentTimeMillis();
			long wendtime = System.currentTimeMillis();
			System.out.println("The size of set is " + set.size() + ", The count is " + count + " time is "
					+ (wendtime - startTime) / 1000);
			wstarttime = System.currentTimeMillis();
			if (count == 2000000) {
				endTime = System.currentTimeMillis();
				System.out.println((endTime - startTime) / 1000);
				System.exit(0);
			}

		}
	}

	private static void insertData(String sourcePath) throws IOException {
		File file = new File(sourcePath);
		int filecount = 0;
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
			exception(e.getMessage());
		}
		Statement statement = null;
		try {
			statement = connection.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		BufferedReader bufferedReader = null;
		filecount++;
		System.out.println("File name is " + file.getCanonicalPath() + " count is " + filecount);
		try {
			bufferedReader = new BufferedReader(new FileReader(file));
			startTime = System.currentTimeMillis();
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				insertOneData(line, statement);
			}
			statement.close();
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void exception(String message) {
		throw new RuntimeException(message);
	}

	private static String[] timeSeries = { "status WITH DATATYPE=BYTE_ARRAY, ENCODING=PLAIN,compressor=SNAPPY",
			"errtype WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"v WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY",
			"a WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY",
			"h WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY",
			"px WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY",
			"py WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY",
			"H_Result WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"A_Result WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"V_Result WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"X_Result WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"Y_Result WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"Bridge_Result WITH DATATYPE=INT32, ENCODING=RLE,compressor=SNAPPY",
			"Pad_W WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY",
			"Pad_H WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF,compressor=SNAPPY" };

	private static String[] timeSeries2 = { "status WITH DATATYPE=BYTE_ARRAY, ENCODING=PLAIN,",
			"errtype WITH DATATYPE=INT32, ENCODING=RLE", "v WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF",
			"a WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF", "h WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF",
			"px WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF", "py WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF",
			"H_Result WITH DATATYPE=INT32, ENCODING=RLE", "A_Result WITH DATATYPE=INT32, ENCODING=RLE",
			"V_Result WITH DATATYPE=INT32, ENCODING=RLE", "X_Result WITH DATATYPE=INT32, ENCODING=RLE",
			"Y_Result WITH DATATYPE=INT32, ENCODING=RLE", "Bridge_Result WITH DATATYPE=INT32, ENCODING=RLE",
			"Pad_W WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF", "Pad_H WITH DATATYPE=FLOAT, ENCODING=TS_2DIFF" };

	private static void createSchema(String filePath, Connection connection) throws IOException {
		File file = new File(filePath);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = bufferedReader.readLine();
		Statement statement = null;
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		while ((line = bufferedReader.readLine()) != null) {
			String[] words = line.split(",");
			String deviceid = "CREATE TIMESERIES root.Inventec." + words[0] + "_" + words[1] + ".";
			for (String s : timeSeries) {
				String sql = deviceid + s;
				try {
					statement.executeUpdate(sql);
				} catch (SQLException e) {
					e.printStackTrace();
					LOGGER.error(e.getMessage());
				}
			}
		}
		bufferedReader.close();
		String fileLevel = "SET STORAGE GROUP TO root.Inventec";
		try {
			statement.executeUpdate(fileLevel);
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		try {
			statement.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
	}
}
