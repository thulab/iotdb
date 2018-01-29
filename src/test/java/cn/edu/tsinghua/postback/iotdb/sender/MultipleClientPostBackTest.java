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
            		timeseriesList.get(storageGroup).add(timeseries);
            	}
            }
        }
        
        file = new File("CreateTimeseries2.txt");
        reader = new BufferedReader(new FileReader(file));
        while ((line = reader.readLine()) != null) {
            String timeseries = line.split(" ")[2];
            for(String storageGroup:timeseriesList.keySet()) {
            	if(timeseries.startsWith(storageGroup + ".")) {
            		timeseriesList.get(storageGroup).add(timeseries);
            	}
            }
        }
        
        for(String storageGroup:timeseriesList.keySet()) {
        	System.out.println(storageGroup);
        	System.out.println(timeseriesList.get(storageGroup).size());
        }
        
		// Compare data of sender and receiver
        for(String storageGroup:timeseriesList.keySet()) {
        	String sqlFormat = "select %s from %s";
        	dataSender.clear();
        	dataReceiver.clear();
        	int count=0;
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					if(!storageGroup.contains("1")) {
						connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.14:6667/", "root", "root");
					}else {
						connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.15:6667/", "root", "root");
					}
					Statement statement = connection.createStatement();
					System.out.println(String.format(sqlFormat,"city_510100.A3T1J0.lng",storageGroup));
					boolean hasResultSet = statement.execute(String.format(sqlFormat,"city_510100.A3T1J0.lng",storageGroup));
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							count++;
						}
					}
					System.out.println(count);
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.16:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute(String.format(sqlFormat,storageGroup));
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							String info = res.getString("Time");
							for(String timeseries:timeseriesList.get(storageGroup)) {
								info = info + res.getString(timeseries);
							}
							dataSender.add(info);
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			System.out.println("Storage Group: " + storageGroup);
			System.out.println("dataSender's size is : " + dataSender.size()); 
			System.out.println(dataSender);
			System.out.println("dataReceiver's size is : " + dataReceiver.size());
			System.out.println(dataReceiver);
			assert((dataSender.size()==dataReceiver.size()) && dataSender.containsAll(dataReceiver));
        }
	}
}
