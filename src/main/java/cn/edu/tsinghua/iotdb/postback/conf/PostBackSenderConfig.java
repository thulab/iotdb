package cn.edu.tsinghua.iotdb.postback.conf;
/**
 * @author lta
 */
import java.io.File;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

public class PostBackSenderConfig {                                    
	
	public static final String CONFIG_NAME = "iotdb-postbackClient.properties";
	
	public String IOTDB_BUFFERWRITE_DIRECTORY = new File(TsfileDBDescriptor.getInstance().getConfig().bufferWriteDir).getAbsolutePath() + File.separator;
	public String UUID_PATH;
	public String LAST_FILE_INFO;
	public String DATA_DIRECTORY;
	public String SNAPSHOT_PATH;
	public String SCHEMA_PATH = new File(TsfileDBDescriptor.getInstance().getConfig().metadataDir).getAbsolutePath() + File.separator + "mlog.txt";
	public String SERVER_IP = "127.0.0.1";
	public int SERVER_PORT = 5555;
	public int CLIENT_PORT = 6666;
	public int UPLOAD_CYCLE_IN_SECONDS = 10;
	public boolean IS_CLEAR_ENABLE = false;
}
