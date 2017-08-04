package cn.edu.thu.tsfiledb.transferfile.transfer.configure;

import java.io.*;
import java.util.Properties;

/**
 * Created by dell on 2017/7/25.
 */
public class ClientConfigure {
    public static int port;
    public static String server_address;
    public static String snapshotDirectory;
    public static Long fileSegmentSize;
    public static Integer clientNTread;
    public static String startTimePath;
    public static String readDBHost;
    public static Integer readDBPort;
    public static String filePositionRecord;

    public static void loadProperties(){
        InputStream inputStream = null;
        File file = new File("settings.properties");
        try {
            inputStream = new FileInputStream(file);
            Properties p = new Properties();
            try {
                p.load(inputStream);
            } catch (IOException e1) {
                System.out.println("fail to load settings!");
            }
            port=Integer.parseInt(p.getProperty("SERVER_PORT"));
            server_address=p.getProperty("SERVER_ADDRESS");
            snapshotDirectory=p.getProperty("SNAPSHOT_DIRECTORY");
            fileSegmentSize=Long.parseLong(p.getProperty("FILE_SEGMENT_SIZE"));
            clientNTread=Integer.parseInt(p.getProperty("CLIENT_NTHREAD"));
            startTimePath=p.getProperty("START_TIME_PATH");
            readDBHost=p.getProperty("READ_DB_HOST");
            readDBPort=Integer.parseInt(p.getProperty("READ_DB_PORT"));
            filePositionRecord=p.getProperty("FILE_RECORD_DIRECTORY");
        } catch (FileNotFoundException e) {
            /**use default parameters*/
        }finally{
            if(inputStream!=null) try {
                inputStream.close();
            } catch (IOException e) {
                System.out.println("close settings.properties fail!");
            }
        }
    }
}