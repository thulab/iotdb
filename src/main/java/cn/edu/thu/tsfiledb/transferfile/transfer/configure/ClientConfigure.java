package cn.edu.thu.tsfiledb.transferfile.transfer.configure;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by dell on 2017/7/25.
 */
public class ClientConfigure {
    public static int port;
    public static String server_address;
    public static String snapshootDirectory;
    public static Long fileSegmentSize;
    public static Integer clientNTread;
    public static String startTimePath;
    public static String readDBHost;
    public static Integer readDBPort;
    public static String filePositionRecord;

    public static void loadProperties() throws FileNotFoundException {
        InputStream inputStream = new FileInputStream("settings.properties");
        Properties p = new Properties();
        try {
            p.load(inputStream);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        port=Integer.parseInt(p.getProperty("SERVER_PORT"));
        server_address=p.getProperty("SERVER_ADDRESS");
        snapshootDirectory=p.getProperty("SNAPSHOOT_DIRECTORY");
        fileSegmentSize=Long.parseLong(p.getProperty("FILE_SEGMENT_SIZE"));
        clientNTread=Integer.parseInt(p.getProperty("CLIENT_NTHREAD"));
        startTimePath=p.getProperty("START_TIME_PATH");
        readDBHost=p.getProperty("READ_DB_HOST");
        readDBPort=Integer.parseInt(p.getProperty("READ_DB_PORT"));
        filePositionRecord=p.getProperty("FILE_RECORD_DIRECTORY");
    }
}