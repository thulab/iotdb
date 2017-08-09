package cn.edu.thu.tsfiledb.transferfile.transfer.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.transferfile.transfer.conf.ServerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lylw on 2017/7/17.
 */
public class Server {
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    
    public static void main(String[] args) {
    	ServerConfig config = ServerConfig.getInstance();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(config.serverNThread);
        ServerSocket serverSocket = null;

        try {
        		serverSocket = new ServerSocket(config.port);
            while(true){
                Socket socket = serverSocket.accept();
                fixedThreadPool.submit(new ReceiveFileThread(socket));
            }
		} catch (IOException e) {
			LOGGER.error("error occurs for server", e);
		} finally {
			if(serverSocket!= null){
				try {
					serverSocket.close();
				} catch (IOException e) {					
				}
			}
		}

    }
}