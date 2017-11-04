package cn.edu.tsinghua.kafka_iotdbDemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

public class SendDataToIotdb {
	Connection connection = null;
	Statement statement = null;
	ResultSet resultSet = null;

	void connectToIotdb() throws Exception {
		// 1. 加载IoTDB的JDBC驱动程序
		Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
		// 2. 使用DriverManager连接IoTDB
		connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
		// 3. 操作IoTDB
		statement = connection.createStatement();
	}

	void sendData(String out) throws Exception {

		// CSV格式文件为逗号分隔符文件，这里根据逗号切分
		String item[] = out.split(",");
		
		// 使用IoTDB-JDBC获取表结构信息
		DatabaseMetaData databaseMetaData = connection.getMetaData();
	    
		//path为将要插入的路径
		String path = "root.vehicle.sensor." + item[0];
		
		//获得path路径集合迭代器
		resultSet = databaseMetaData.getColumns(null, null, path, null);
		
	    //如果路径迭代器中为null，则表示path不存在，需创建
	    if(!resultSet.next())
	    {
			String epl = "CREATE TIMESERIES " + path + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
			statement.execute(epl);
	    }
		//向IoTDB插入数据
		String template = "INSERT INTO root.vehicle.sensor(timestamp,%s) VALUES (%s,'%s')";
		String epl = String.format(template, item[0], item[1], item[2]);
		statement.execute(epl);
	}

	public static void main(String[] args) throws Exception {
		SendDataToIotdb sendDataToIotdb = new SendDataToIotdb();
		sendDataToIotdb.connectToIotdb();
		sendDataToIotdb.sendData("sensor4,2017/10/24 19:33:00,121 93 99");
	}
}

