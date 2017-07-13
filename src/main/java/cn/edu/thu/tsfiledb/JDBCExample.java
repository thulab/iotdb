package cn.edu.thu.tsfiledb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCExample {
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCExample.class);
	private static String schemaFilePath = "src/main/resources/schema.csv";
	private static String sqlFile = "src/main/resources/sqlfile";
	private static BufferedWriter bufferedWriter = null;
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

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		bufferedWriter = new BufferedWriter(new FileWriter(new File(sqlFile)));
		insertData(sourceFilePath);
		bufferedWriter.close();
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
		if (line.indexOf('E') != -1) {
			return;
		}
		String[] words = line.split(",");
		if (words.length != 18) {
			return;
		}
		for (int i = 0; i < words.length; i++) {
			String word = words[i];
			if (word.startsWith(".")) {
				words[i] = "0" + word;
			}
			if (word.startsWith("-.")) {
				words[i] = "-0" + word.substring(1, word.length());
			}
		}
		words[0] = timeTrans(words[0]);
		// String insertSql = String.format(
		// "insert into root.Inventec.%s_%s"
		// +
		// "(timestamp,status,errtype,v,a,h,px,py,H_Result,A_Result,V_Result,X_Result,Y_Result,Bridge_Result,Pad_W,Pad_H)
		// "
		// + "VALUES (%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
		// words[1], words[2], words[0], words[3], words[4], words[5], words[6],
		// words[7], words[8], words[9],
		// words[10], words[11], words[12], words[13], words[14], words[15],
		// words[16], words[17]);
		String insertSql = String.format(
				"insert into root.Inventec.%s_%s"
						+ "(timestamp,errtype,v,a,h,px,py,H_Result,A_Result,V_Result,X_Result,Y_Result,Bridge_Result,Pad_W,Pad_H) "
						+ "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
				words[1], words[2], words[0], words[4], words[5], words[6], words[7], words[8], words[9], words[10],
				words[11], words[12], words[13], words[14], words[15], words[16], words[17]);

		set.add(words[1] + "_" + words[2]);
		count++;
		if (count == 12000) {
			System.out.println("should close");
		}
		if (count % 10000 == 0) {
			System.out.println("The size of set is " + set.size() + ", The count is " + count);
			if (count == 10000000) {
				try {
					endTime = System.currentTimeMillis();
					System.out.println((endTime - startTime) / 1000);
					bufferedWriter.flush();
					bufferedWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.exit(0);
			}
		}
		try {
			bufferedWriter.write(insertSql);
			bufferedWriter.write("\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void insertData(String sourcePath) throws IOException {
		File file = new File(sourcePath);
		if (!file.isDirectory()) {
			throw new RuntimeException("Source File is not directory");
		}
		int filecount = 0;
		for (File subFile : file.listFiles()) {
			Statement statement = null;
			BufferedReader bufferedReader = null;
			filecount++;
			System.out.println("File name is " + subFile.getCanonicalPath() + " count is " + filecount);
			try {
				bufferedReader = new BufferedReader(new FileReader(subFile));
				startTime = System.currentTimeMillis();
				String line = bufferedReader.readLine();
				while ((line = bufferedReader.readLine()) != null) {
					insertOneData(line, statement);
				}
			} catch (FileNotFoundException e) {
				LOGGER.error(e.getMessage());
			}
		}
	}

}
