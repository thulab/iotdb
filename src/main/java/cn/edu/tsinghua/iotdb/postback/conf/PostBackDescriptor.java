package cn.edu.tsinghua.iotdb.postback.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;

public class PostBackDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(PostBackDescriptor.class);

	private static class PostBackDescriptorHolder{
		private static final PostBackDescriptor INSTANCE = new PostBackDescriptor();
	}
	
	private PostBackDescriptor() {
		loadProps();
	}

	public static final PostBackDescriptor getInstance() {
		return PostBackDescriptorHolder.INSTANCE;
	}

	public PostBackConfig getConfig() {
		return conf;
	}
	
	public void setConfig(PostBackConfig conf) {
		this.conf = conf;
	}

	private PostBackConfig conf = new PostBackConfig();

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
				url = url + File.separatorChar + "conf" + File.separatorChar + PostBackConfig.CONFIG_NAME;
			} else {
				LOGGER.warn("Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration", PostBackConfig.CONFIG_NAME);
				return;
			}
		} else{
			url += (File.separatorChar + PostBackConfig.CONFIG_NAME);
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

			conf.UPLOAD_CYCLE_IN_SECONDS = Integer.parseInt(properties.getProperty("upload_cycle_in_seconds", conf.UPLOAD_CYCLE_IN_SECONDS+""));

			conf.IS_UPLOAD_ENABLE = Boolean.parseBoolean(properties.getProperty("is_upload_enable", conf.IS_UPLOAD_ENABLE+""));
			
			conf.POSTBACK_TYPE = properties.getProperty("postback_type",conf.POSTBACK_TYPE);
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