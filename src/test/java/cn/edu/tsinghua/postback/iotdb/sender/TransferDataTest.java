package cn.edu.tsinghua.postback.iotdb.sender;

import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.postback.iotdb.receiver.ServerManager;


public class TransferDataTest {

	private String POST_BACK_DIRECTORY_TEST = new File("postback" + File.separator).getAbsolutePath() + File.separator;
	private String UUID_PATH_TEST = POST_BACK_DIRECTORY_TEST + "uuid.txt";
	private String LAST_FILE_INFO_TEST = POST_BACK_DIRECTORY_TEST + "lastLocalFileList.txt";
	private String SENDER_FILE_PATH_TEST = POST_BACK_DIRECTORY_TEST + "data";
	private String SNAPSHOT_PATH_TEST = POST_BACK_DIRECTORY_TEST + "dataSnapshot";
	private String SERVER_IP_TEST = "127.0.0.1";
	private int SERVER_PORT_TEST = 5555;
	private PostBackSenderConfig config = PostBackSenderDescriptor.getInstance().getConfig();
	ServerManager serverManager = ServerManager.getInstance();
	TransferData transferData = TransferData.getInstance();
	FileManager manager = FileManager.getInstance();

	public void setConfig() {
		config.IOTDB_DATA_DIRECTORY = POST_BACK_DIRECTORY_TEST;
		config.UUID_PATH = UUID_PATH_TEST;
		config.LAST_FILE_INFO = LAST_FILE_INFO_TEST;
		config.SNAPSHOT_PATH = SNAPSHOT_PATH_TEST;
		config.SERVER_IP = SERVER_IP_TEST;
		config.SERVER_PORT = SERVER_PORT_TEST;
		transferData.setConfig(config);
	}
	
	private IoTDB deamon;
	private List<String> schemaToSend = new ArrayList<>();
	
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

            "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
            "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
            "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
            "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
            "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
            "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
            "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
            "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
            "DELETE FROM root.vehicle.d0.s0 WHERE time < 104",
            "UPDATE root.vehicle SET d0.s0 = 33333 WHERE time < 106 and time > 103",

            "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
            "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
            "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
            "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
            "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
            "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
            "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
            "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",

            "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
            "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
            "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
            "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
            "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
            "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
            "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",

            "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
            "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
            "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
            "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
            "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",
            "UPDATE root.vehicle SET d0.s3 = 'tomorrow is another day' WHERE time >100 and time < 103",

            "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
            "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",

            "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
            "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

            "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
            "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
    };

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
    	setConfig();
        if (testFlag) {
        	EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
        serverManager.startServer();
		File file =new File(config.LAST_FILE_INFO);
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		if (!file.exists()) {
			file.createNewFile();
		}
		file =new File(config.SENDER_FILE_PATH);
		if (!file.exists()) {
			file.mkdirs();
		}
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(5000);
            EnvironmentUtils.cleanEnv();
        }
        serverManager.closeServer();
		delete(new File(config.IOTDB_DATA_DIRECTORY));
		new File(config.IOTDB_DATA_DIRECTORY).delete();
		String RECEIVER_POST_BACK_DIRECTORY = config.IOTDB_DATA_DIRECTORY + transferData.getUuid();
		if(new File(RECEIVER_POST_BACK_DIRECTORY).exists())
		{
			delete(new File(RECEIVER_POST_BACK_DIRECTORY));
			new File(RECEIVER_POST_BACK_DIRECTORY).delete();
		}
    }
    
    public void delete(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} 
		else{
			File[] files = file.listFiles();
			for (File f : files) {
				delete(f);        
				f.delete();       
			}
		}		
	}

    @Test
	public void testTransferUUID() {
		String uuidOfSender;
		String uuidOfReceiver;
		transferData.connection(config.SERVER_IP, config.SERVER_PORT);
		
		//generate uuid and write it to file
		uuidOfReceiver = transferData.transferUUID(config.UUID_PATH);
		uuidOfSender = transferData.getUuid();
		assert(uuidOfReceiver.equals(uuidOfSender));
		
		//read uuid from file
		serverManager.closeServer();
		serverManager.startServer();
		transferData.connection(config.SERVER_IP, config.SERVER_PORT);
		uuidOfReceiver = transferData.transferUUID(config.UUID_PATH);
		uuidOfSender = transferData.getUuid();
		assert(uuidOfReceiver.equals(uuidOfSender));
	}

	@Test
	public void testFileSnapshot() throws Exception {
 		Map<String,Set<String>> sendingFileList = new HashMap<>();
 		Set<String> lastlocalList = new HashSet<>();
 		Map<String,Set<String>> fileList = new HashMap<>();
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
					FileOutputStream out = new FileOutputStream(file);
					out.write((i + " " + j).getBytes());
					out.close();
				}
			}
		}
		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		lastlocalList = manager.getLastLocalFiles();
 		manager.setNowLocalFiles(new HashMap<>());
		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		fileList = manager.getNowLocalFiles();
		manager.getSendingFileList();
 		sendingFileList = manager.getSendingFiles();
 		for(Entry<String, Set<String>> entry:sendingFileList.entrySet()) {
			transferData.makeFileSnapshot(entry.getValue(), SNAPSHOT_PATH_TEST, POST_BACK_DIRECTORY_TEST);
		}
 		//compare all md5 of source files and snapshot files
 		for(Entry<String, Set<String>> entry:sendingFileList.entrySet()) {
	 		for(String filePath:entry.getValue())
	 		{
	 			String md5OfSource = getMD5(new File(filePath));
	 			String relativeFilePath = filePath.substring(POST_BACK_DIRECTORY_TEST.length());
	 			String newPath = SNAPSHOT_PATH_TEST + File.separator + relativeFilePath;
	 			String md5OfSnapshot = getMD5(new File(newPath));
	 			assert(md5OfSource.equals(md5OfSnapshot));
	 		}
 		}
	}
	
	public String getMD5(File file) throws Exception
	{
		FileInputStream fis = new FileInputStream(file);
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] buffer = new byte[1024];
		int length = -1;
		while ((length = fis.read(buffer, 0, 1024)) != -1) {
			md.update(buffer, 0, length);
		}
		String md5 = (new BigInteger(1, md.digest())).toString(16);
		fis.close();
		return md5;
	}
	
	@Test
	public void testAfterSending() throws Exception {
		Map<String,Set<String>> sendingFileList = new HashMap<>();
 		Set<String> lastlocalList = new HashSet<>();
 		Map<String,Set<String>> fileList = new HashMap<>();
 		
		transferData.connection(SERVER_IP_TEST, SERVER_PORT_TEST);
		transferData.transferUUID(UUID_PATH_TEST);
		
		Random r = new Random(0);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				String rand = String.valueOf(r.nextInt(10000));
				String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
				File file = new File(fileName);
				if (!file.getParentFile().exists()) {
					file.getParentFile().mkdirs();
				}
				if (!file.exists()) {
					file.createNewFile();
					FileOutputStream out = new FileOutputStream(file);
					out.write((i + " " + j).getBytes());
					out.close();
				}
			}
		}
		manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
		lastlocalList = manager.getLastLocalFiles();
 		manager.setNowLocalFiles(new HashMap<>());
		manager.getNowLocalFileList(SENDER_FILE_PATH_TEST);
		fileList = manager.getNowLocalFiles();
		manager.getSendingFileList();
 		sendingFileList = manager.getSendingFiles();
 		for(Entry<String, Set<String>> entry:sendingFileList.entrySet()) {
			transferData.makeFileSnapshot(entry.getValue(), SNAPSHOT_PATH_TEST, POST_BACK_DIRECTORY_TEST);
		}
 		Thread.sleep(1000);
 		transferData.deleteSnapshot(new File(SNAPSHOT_PATH_TEST));
 		assert(new File(SNAPSHOT_PATH_TEST).list().length==0);
	}

	private void insertSQL() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            Statement statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }
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
