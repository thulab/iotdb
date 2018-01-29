package cn.edu.tsinghua.postback.iotdb.receiver;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class ServiceImpTest {
	private IoTDB deamon;	
	private TsfileDBConfig conf = TsfileDBDescriptor.getInstance().getConfig();
	private String[] sqls = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
            "CREATE TIMESERIES root.vehicle.d2 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE",

            "insert into root.vehicle.d1(timestamp,s0) values(1,101)",
            "insert into root.vehicle(timestamp,d2) values(1,121)",
            "insert into root.vehicle.d0(timestamp,s0,s1,s2) values(2,198,199,200.0)",
            "insert into root.vehicle.d0(timestamp,s0,s2,s3) values(100,99,199.0,'aaaaa')",
            "insert into root.vehicle.d0(timestamp,s0,s1,s2,s3,s4) values(101,99,199,200.0,'aaaaa',true)",
            "insert into root.vehicle.d0(timestamp,s2,s4) values(102,80.0,true)",
            "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
            "merge",
            "flush",
    };

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
        	EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);
            EnvironmentUtils.cleanEnv();
        }
    }

	@Test
	public void testMergeBySQL() {
		if (testFlag) {
            try {
                Thread.sleep(5000);
                insertSQL();
                
                Set<String> insertSQL = new HashSet<>();
                insertSQL.clear();
                
                for(String sql:sqls)
                {
                	if(sql.toLowerCase().startsWith("insert"))
                	{
                		insertSQL.add(sql);
                	}
                }
                Map<String,List<String>> oldFilesMap = new HashMap<>();
                oldFilesMap.put("root.vehicle", new ArrayList<String>());
                String path = conf.bufferWriteDir + File.separator + "root.vehicle" + File.separator;
                File files = new File(path);
                File flist[] = files.listFiles();
                for(File file:flist)
                {
                	oldFilesMap.get("root.vehicle").add(file.getAbsolutePath());
                }
                ServiceImp serviceImp = new ServiceImp();
                serviceImp.setOldFilesMap(oldFilesMap);
                try {
					serviceImp.getSqlToMerge();
				} catch (TException e) {
					e.printStackTrace();
				}
                Set<String> SQLToMerge = serviceImp.getSQLToMerge();
                System.out.println("SQLTOMERGE:");
                for(String sql: SQLToMerge)
                System.out.println(sql);
                System.out.println("insertSQL:");
                for(String sql:insertSQL)
                System.out.println(sql);
                assert(insertSQL.size() == SQLToMerge.size() && insertSQL.containsAll(SQLToMerge));
            } catch (ClassNotFoundException | SQLException | InterruptedException e) {
                fail(e.getMessage());
            }
        }
	}
	
	private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
                System.out.println(sql);
            }
            statement.execute("flush");
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
