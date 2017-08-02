package cn.edu.thu.tsfiledb.transferfile.transfer.configure;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by dell on 2017/7/25.
 */
public class ServerConfigure {

    public static String storage_directory;
    public static int port;
    public static Integer server_NThread;

    public static void loadProperties() throws FileNotFoundException {
        InputStream inputStream = new FileInputStream("settings.properties");
        Properties p = new Properties();
        try {
            p.load(inputStream);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        storage_directory = p.getProperty("STORAGE_DIRECTORY");
        port = Integer.parseInt(p.getProperty("SERVER_PORT"));

        server_NThread=Integer.parseInt(p.getProperty("SERVER_NTHREAD"));
    }
}
