package cn.edu.thu.tsfiledb.transferfile.transfer.server;

import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ServerConfigure;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lylw on 2017/7/17.
 */
public class Server {

    public static void main(String[] args) throws IOException, InterruptedException {
        /**read settings from settings.properties*/
        ServerConfigure.loadProperties();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(ServerConfigure.server_NThread);
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(ServerConfigure.port);
        } catch (IOException e) {
            System.out.println("fail to get ServerSocket!");
        }
        while(true){
            Socket socket = serverSocket.accept();
            fixedThreadPool.submit(new ReceiveFiles(socket));
        }
    }
}