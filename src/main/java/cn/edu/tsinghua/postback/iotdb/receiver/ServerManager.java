package cn.edu.tsinghua.postback.iotdb.receiver;
/**
 * @author lta
 */
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;

public class ServerManager {
	private TServerSocket serverTransport;
	private Factory protocolFactory;
	private TProcessor processor;
	private TThreadPoolServer.Args poolArgs;
	private TServer poolServer;
	
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
			serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().POSTBACK_SERVER_PORT);
			protocolFactory = new TBinaryProtocol.Factory();
			processor = new Service.Processor<ServiceImp>(new ServiceImp());
			poolArgs = new TThreadPoolServer.Args(serverTransport);
			poolArgs.processor(processor);
			poolArgs.protocolFactory(protocolFactory);
			poolServer = new TThreadPoolServer(poolArgs);
			LOGGER.info("Postback server start!");
			Runnable runnable = new Runnable() {
				public void run() {
					poolServer.serve();
				}
			};
			new Thread(runnable).start();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back receicer: cannot start postback server because {}", e.getMessage());
		}
	}

	public void closeServer() {
		poolServer.stop();
		LOGGER.info("Stop postback server!");
	}
	
	public static void main(String[] args) {
		ServerManager serverManager = ServerManager.getInstance();
		serverManager.startServer();
	}
}