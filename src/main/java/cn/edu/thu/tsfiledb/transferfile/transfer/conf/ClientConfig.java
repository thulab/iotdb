package cn.edu.thu.tsfiledb.transferfile.transfer.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.constant.SystemConstant;

/**
 * Created by dell on 2017/7/25.
 */
public class ClientConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientConfig.class);
	private static final String CONFIG_NAME = "data-collect-client.properties";
	private static final String CONFIG_DEFAULT_PATH = "tsfiledb/conf/" + CONFIG_NAME;

	private static class ClientConfigHolder {
		private static final ClientConfig INSTANCE = new ClientConfig();
	}

	private ClientConfig() {
		loadProperties();
	}

	public static final ClientConfig getInstance() {
		return ClientConfigHolder.INSTANCE;
	}

	public int port = 10086;
	public String serverAddress = "127.0.0.1";
	public String snapshotDirectory = "testFile";
	public long fileSegmentSize = 128;
	public int clientNTread = 5;
	public String startTimePath = "tsfiledb_test/startTime";
	public String readDBHost = "127.0.0.1";
	public int readDBPort = 6668;
	public String filePositionRecord = "tsfiledb_test/bytePosition";

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
			fileSegmentSize = Long.parseLong(p.getProperty("FILE_SEGMENT_SIZE", fileSegmentSize+""));
			clientNTread = Integer.parseInt(p.getProperty("CLIENT_NTHREAD", clientNTread+""));
			startTimePath = p.getProperty("START_TIME_PATH", startTimePath);
			readDBHost = p.getProperty("READ_DB_HOST", readDBHost);
			readDBPort = Integer.parseInt(p.getProperty("READ_DB_PORT", readDBPort+""));
			filePositionRecord = p.getProperty("FILE_RECORD_DIRECTORY", filePositionRecord);
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
    		ClientConfig config = ClientConfig.getInstance();
        System.out.println(config.port);
    }
}