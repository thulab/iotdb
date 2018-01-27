package cn.edu.tsinghua.postback.iotdb.receiver;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.postback.conf.PostBackConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackDescriptor;

public class ServerManager {
	private TServerSocket serverTransport;
	private Factory protocolFactory;
	private TProcessor processor;
	private TThreadPoolServer.Args poolArgs;
	private TServer poolServer;
	private PostBackConfig config= PostBackDescriptor.getInstance().getConfig();
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);
	private static class ServerManagerHolder{
		private static final ServerManager INSTANCE = new ServerManager();
	}
	
	private ServerManager() {}

	public static final ServerManager getInstance() {
		return ServerManagerHolder.INSTANCE;
	}

	public void startServer() {
		try {
			serverTransport = new TServerSocket(config.SERVER_PORT);
			protocolFactory = new TBinaryProtocol.Factory();
			processor = new Service.Processor<ServiceImp>(new ServiceImp());
			poolArgs = new TThreadPoolServer.Args(serverTransport);
			poolArgs.processor(processor);
			poolArgs.protocolFactory(protocolFactory);
			poolServer = new TThreadPoolServer(poolArgs);
			LOGGER.info("Thrift Server start!");
			Runnable runnable = new Runnable() {
				public void run() {
					poolServer.serve();
				}
			};
			new Thread(runnable).start();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back receicer: cannot start server because {}", e.getMessage());
		}
	}

	public void closeServer() {
		poolServer.stop();
		LOGGER.info("Stop thrift server!");
	}
	
	public static void main(String[] args) {
		ServerManager serverManager = ServerManager.getInstance();
		serverManager.startServer();
	}
}