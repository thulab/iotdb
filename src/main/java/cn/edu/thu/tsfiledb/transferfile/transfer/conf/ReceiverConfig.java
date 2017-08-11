package cn.edu.thu.tsfiledb.transferfile.transfer.conf;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by dell on 2017/7/25.
 */
public class ReceiverConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverConfig.class);
	private static final String CONFIG_NAME = "data-collect-receiver.properties";
	private static final String CONFIG_DEFAULT_PATH = "tsfiledb/conf/" + CONFIG_NAME;
	
	private static class ServerConfigHolder {
		private static final ReceiverConfig INSTANCE = new ReceiverConfig();
	}

	private ReceiverConfig() {
		loadProperties();
	}

	public static final ReceiverConfig getInstance() {
		return ServerConfigHolder.INSTANCE;
	}

	public String storageDirectory = "receiveFile";
	public int port = 10086;
	public int serverNThread = 5;
	public int fileSegmentSize = 4096;

	public void loadProperties() {
		String tsfileHome = System.getProperty(SystemConstant.TSFILE_HOME, CONFIG_DEFAULT_PATH);
		String url;
		InputStream inputStream = null;
		if (tsfileHome.equals(CONFIG_DEFAULT_PATH)) {
			url = tsfileHome;
			try {
			    inputStream = new FileInputStream(new File(url));
			} catch (Exception e) {
			    LOGGER.warn("Fail to find config file {}", url, e);
			    return;
			}
			
		} else {
			url = tsfileHome + File.separatorChar+"conf"+ File.separatorChar+CONFIG_NAME;
			try {
				File file = new File(url);
				inputStream = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				LOGGER.warn("Fail to find config file {}", url, e);
				return;
			}
		}
		LOGGER.info("Start to read config file {}", url);
		Properties p = new Properties();
		try {
			p.load(inputStream);
			storageDirectory = p.getProperty("STORAGE_DIRECTORY", storageDirectory);
			port = Integer.parseInt(p.getProperty("SERVER_PORT", port+""));
			serverNThread = Integer.parseInt(p.getProperty("SERVER_NTHREAD", serverNThread+""));
			fileSegmentSize = Integer.parseInt(p.getProperty("FILE_SEGMENT_SIZE",fileSegmentSize+""));
		} catch (IOException e) {
			LOGGER.warn("Cannot load config file, use default configuration", e);
		} catch (Exception e) {
			LOGGER.warn("Error format in config file, use default configuration", e);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					LOGGER.error("Fail to close config file input stream", e);
				}
			}
		}
	}
	
    public static void main(String[] args) {
        ReceiverConfig config = ReceiverConfig.getInstance();
        System.out.println(config.port);
    }
}
