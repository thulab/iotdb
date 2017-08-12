package cn.edu.thu.tsfiledb.transferfile.transfer.conf;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by dell on 2017/7/25.
 */
public class SenderConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(SenderConfig.class);
	private static final String CONFIG_NAME = "data-collect-sender.properties";
	private static final String CONFIG_DEFAULT_PATH = "tsfiledb/conf/" + CONFIG_NAME;

	private static class ClientConfigHolder {
		private static final SenderConfig INSTANCE = new SenderConfig();
	}

	private SenderConfig() {
		loadProperties();
	}

	public static final SenderConfig getInstance() {
		return ClientConfigHolder.INSTANCE;
	}

	public int port = 10086;
	public String serverAddress = "127.0.0.1";
	public String snapshotDirectory = "testFile";
	public int fileSegmentSize = 128;
	public int clientNTread = 5;
	public String startTimePath = "tsfiledb_test/startTime";
	public String readDBHost = "127.0.0.1";
	public int readDBPort = 6668;
	public String filePositionRecord = "tsfiledb_test/bytePosition";
	public long transferTimeInterval = 60000L;

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
			url = tsfileHome + File.separatorChar + "conf" + File.separatorChar + CONFIG_NAME;
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
			port = Integer.parseInt(p.getProperty("SERVER_PORT", port+""));
			serverAddress = p.getProperty("SERVER_ADDRESS", serverAddress);
			snapshotDirectory = p.getProperty("SNAPSHOT_DIRECTORY", snapshotDirectory);
			fileSegmentSize = Integer.parseInt(p.getProperty("FILE_SEGMENT_SIZE", fileSegmentSize+""));
			clientNTread = Integer.parseInt(p.getProperty("CLIENT_NTHREAD", clientNTread+""));
			startTimePath = p.getProperty("START_TIME_PATH", startTimePath);
			readDBHost = p.getProperty("READ_DB_HOST", readDBHost);
			readDBPort = Integer.parseInt(p.getProperty("READ_DB_PORT", readDBPort+""));
			filePositionRecord = p.getProperty("FILE_RECORD_DIRECTORY", filePositionRecord);
			transferTimeInterval = Long.parseLong(p.getProperty("TRANSFER_TIME_INTERVAL"));
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
		SenderConfig config = SenderConfig.getInstance();
        System.out.println(config.port);
    }
}