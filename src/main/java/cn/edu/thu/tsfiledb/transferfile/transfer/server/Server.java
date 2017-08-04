package cn.edu.thu.tsfiledb.transferfile.transfer.server;

import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ServerConfigure;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lylw on 2017/7/17.
 */
public class Server {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Server.class);
    public static void main(String[] args) throws IOException, InterruptedException {
        /**read settings from settings.properties*/
        ServerConfigure.loadProperties();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(ServerConfigure.server_NThread);
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(ServerConfigure.port);
        } catch (IOException e) {
            LOGGER.error("fail to get ServerSocket!");
        }
        while(true){
            Socket socket = serverSocket.accept();
            fixedThreadPool.submit(new ReceiveFiles(socket));
        }
    }
}