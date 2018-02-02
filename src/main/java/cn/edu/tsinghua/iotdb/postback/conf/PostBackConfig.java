package cn.edu.tsinghua.iotdb.postback.conf;

import java.io.File;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

public class PostBackConfig {                                    
	
	public static final String CONFIG_NAME = "iotdb-postback.properties";
	
	public String IOTDB_DATA_DIRECTORY = new File(TsfileDBDescriptor.getInstance().getConfig().dataDir).getAbsolutePath() + File.separator;
	public String UUID_PATH = IOTDB_DATA_DIRECTORY + "uuid.txt";
	public String LAST_FILE_INFO = IOTDB_DATA_DIRECTORY + "lastLocalFileList.txt";
	public String SENDER_FILE_PATH = IOTDB_DATA_DIRECTORY + "delta";
	public String SNAPSHOT_PATH = IOTDB_DATA_DIRECTORY + "dataSnapshot";
	public String SCHEMA_PATH = IOTDB_DATA_DIRECTORY + "metadata" + File.separator + "mlog.txt";
	public String SERVER_IP = "192.168.130.16";
	public int SERVER_PORT = 5555;
	public int UPLOAD_CYCLE_IN_SECONDS = 10;
	public boolean IS_UPLOAD_ENABLE = true;
	public String POSTBACK_TYPE = "Client";
}
