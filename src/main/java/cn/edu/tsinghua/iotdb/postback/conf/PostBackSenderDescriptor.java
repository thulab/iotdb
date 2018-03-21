package cn.edu.tsinghua.iotdb.postback.conf;
/**
 * @author lta
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;

public class PostBackSenderDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PostBackSenderDescriptor.class);

	private static class PostBackDescriptorHolder{
		private static final PostBackSenderDescriptor INSTANCE = new PostBackSenderDescriptor();
	}
	
	private PostBackSenderDescriptor() {
		loadProps();
	}

	public static final PostBackSenderDescriptor getInstance() {
		return PostBackDescriptorHolder.INSTANCE;
	}

	public PostBackSenderConfig getConfig() {
		return conf;
	}
	
	public void setConfig(PostBackSenderConfig conf) {
		this.conf = conf;
	}

	private PostBackSenderConfig conf = new PostBackSenderConfig();

	/**
	 * load an properties file and set TsfileDBConfig variables
	 *
	 */
	private void loadProps() {
		InputStream inputStream = null;
		String url = System.getProperty(TsFileDBConstant.IOTDB_CONF, null);
		if (url == null) {
			url = System.getProperty(TsFileDBConstant.IOTDB_HOME, null);
			if (url != null) {
				url = url + File.separatorChar + "conf" + File.separatorChar + PostBackSenderConfig.CONFIG_NAME;
			} else {
				LOGGER.warn("Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration", PostBackSenderConfig.CONFIG_NAME);
				return;
			}
		} else{
			url += (File.separatorChar + PostBackSenderConfig.CONFIG_NAME);
		}
		
		try {
			inputStream = new FileInputStream(new File(url));
		} catch (FileNotFoundException e) {
			LOGGER.warn("Fail to find config file {}", url);
			// update all data path
			return;
		}

		LOGGER.info("Start to read config file {}", url);
		Properties properties = new Properties();
		try {
			properties.load(inputStream);
			
			conf.SERVER_IP = properties.getProperty("server_ip",conf.SERVER_IP);
			
			conf.SERVER_PORT = Integer.parseInt(properties.getProperty("server_port", conf.SERVER_PORT+""));
			
			conf.CLIENT_PORT = Integer.parseInt(properties.getProperty("client_port", conf.SERVER_PORT+""));

			conf.UPLOAD_CYCLE_IN_SECONDS = Integer.parseInt(properties.getProperty("upload_cycle_in_seconds", conf.UPLOAD_CYCLE_IN_SECONDS+""));

			conf.IOTDB_DATA_DIRECTORY = properties.getProperty("iotdb_data_directory", conf.IOTDB_DATA_DIRECTORY);
			
			conf.IS_CLEAR_ENABLE = Boolean.parseBoolean(properties.getProperty("is_clear_enable", conf.IS_CLEAR_ENABLE + ""));
			
			if(!conf.IOTDB_DATA_DIRECTORY.endsWith(File.separator))
				conf.IOTDB_DATA_DIRECTORY = conf.IOTDB_DATA_DIRECTORY + File.separator;
			
			conf.UUID_PATH = conf.IOTDB_DATA_DIRECTORY + "uuid.txt";
			conf.LAST_FILE_INFO = conf.IOTDB_DATA_DIRECTORY + "lastLocalFileList.txt";
			conf.SENDER_FILE_PATH = conf.IOTDB_DATA_DIRECTORY + "delta";
			conf.SNAPSHOT_PATH = conf.IOTDB_DATA_DIRECTORY + "dataSnapshot";
			conf.SCHEMA_PATH = conf.IOTDB_DATA_DIRECTORY + "metadata" + File.separator + "mlog.txt";
			
			
		} catch (IOException e) {
			LOGGER.warn("Cannot load config file because {}, use default configuration", e.getMessage());
		} catch (Exception e) {
			LOGGER.warn("Error format in config file because {}, use default configuration", e.getMessage());
		}
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				LOGGER.error("Fail to close config file input stream because {}", e.getMessage());
			}
		}
	}
}