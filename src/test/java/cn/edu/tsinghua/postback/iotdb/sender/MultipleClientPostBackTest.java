package cn.edu.tsinghua.postback.iotdb.sender;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class MultipleClientPostBackTest {

	Map<String,ArrayList<String>> timeseriesList = new HashMap();
	private Set<String> dataSender = new HashSet<>();
	private Set<String> dataReceiver = new HashSet<>();
	
	@Before
	public void setUp() throws Exception {
		
	}

	@After
	public void tearDown() throws Exception {
		
	}
	
	@Test
	public void testPostback() throws IOException {
		
		timeseriesList.clear();
		timeseriesList.put("root.vehicle_history1",new ArrayList<String>());
        timeseriesList.put("root.vehicle_alarm1",new ArrayList<String>());
        timeseriesList.put("root.vehicle_temp1",new ArrayList<String>());
        timeseriesList.put("root.range_event1",new ArrayList<String>());
        timeseriesList.put("root.vehicle_history",new ArrayList<String>());
        timeseriesList.put("root.vehicle_alarm",new ArrayList<String>());
        timeseriesList.put("root.vehicle_temp",new ArrayList<String>());
        timeseriesList.put("root.range_event",new ArrayList<String>());
		
        File file = new File("CreateTimeseries1.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
            String timeseries = line.split(" ")[2];
            for(String storageGroup:timeseriesList.keySet()) {
            	if(timeseries.startsWith(storageGroup + ".")) {
            		String timesery = timeseries.substring((storageGroup + ".").length());
            		timeseriesList.get(storageGroup).add(timesery);
            		break;
            	}
            }
        }
        
        file = new File("CreateTimeseries2.txt");
        reader = new BufferedReader(new FileReader(file));
        while ((line = reader.readLine()) != null) {
            String timeseries = line.split(" ")[2];
            for(String storageGroup:timeseriesList.keySet()) {
            	if(timeseries.startsWith(storageGroup + ".")) {
            		String timesery = timeseries.substring((storageGroup + ".").length());
            		timeseriesList.get(storageGroup).add(timesery);
            		break;
            	}
            }
        }
        
		// Compare data of sender and receiver
        for(String storageGroup:timeseriesList.keySet()) {
        	String sqlFormat = "select %s from %s";
        	System.out.println(storageGroup + ":");
        	int count=0;
        	for(String timesery:timeseriesList.get(storageGroup)) {
            	count++; 
        		dataSender.clear();
            	dataReceiver.clear();
        		try {
					Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
					Connection connection = null;
					Connection connection1 = null;
					try {
						if(!storageGroup.contains("1")) {
							connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.14:6667/", "root", "root");
						}else {
							connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.15:6667/", "root", "root");
						}
						connection1 = DriverManager.getConnection("jdbc:tsfile://192.168.130.16:6667/", "root", "root");
						Statement statement = connection.createStatement();
						Statement statement1 = connection1.createStatement();
						String SQL = String.format(sqlFormat, timesery, storageGroup);
						boolean hasResultSet = statement.execute(SQL);
						boolean hasResultSet1 = statement1.execute(SQL);
						if (hasResultSet) {
							ResultSet res = statement.getResultSet();
							while (res.next()) {
								dataSender.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
							}
						}
						if (hasResultSet1) {
							ResultSet res = statement1.getResultSet();
							while (res.next()) {
								dataReceiver.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
							}
						}
						assert((dataSender.size()==dataReceiver.size()) && dataSender.containsAll(dataReceiver));
						statement.close();
						statement1.close();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (connection != null) {
							connection.close();
						}
						if (connection1 != null) {
							connection1.close();
						}
					}
				} catch (ClassNotFoundException | SQLException e) {
					fail(e.getMessage());
				}
            	if(count > 100)
            		break;
        	}
        }
	}
}
