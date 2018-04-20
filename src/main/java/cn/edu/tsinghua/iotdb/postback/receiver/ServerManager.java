package cn.edu.tsinghua.iotdb.postback.receiver;
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

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

public class ServerManager {
	private TServerSocket serverTransport;
	private Factory protocolFactory;
	private TProcessor processor;
	private TThreadPoolServer.Args poolArgs;
	private TServer poolServer;
	private TsfileDBConfig conf = TsfileDBDescriptor.getInstance().getConfig();
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);
	private static class ServerManagerHolder{
		private static final ServerManager INSTANCE = new ServerManager();
	}
	
	private ServerManager() {}

	public static final ServerManager getInstance() {
		return ServerManagerHolder.INSTANCE;
	}

	public void startServer() {
		if (!conf.IS_POSTBACK_ENABLE) {
			return;
		}
		try {
			if(conf.IP_white_list == null) {
				LOGGER.error("IoTDB post back receicer: Postback server failed to start because IP white list is null, please set IP white list!");
				return;
			}
			else {
				conf.IP_white_list = conf.IP_white_list.replaceAll(" ", "");
			}
			serverTransport = new TServerSocket(conf.POSTBACK_SERVER_PORT);
			protocolFactory = new TBinaryProtocol.Factory();
			processor = new Service.Processor<ServiceImp>(new ServiceImp());
			poolArgs = new TThreadPoolServer.Args(serverTransport);
			poolArgs.processor(processor);
			poolArgs.protocolFactory(protocolFactory);
			poolServer = new TThreadPoolServer(poolArgs);
			LOGGER.info("Postback server has started!");
			Runnable runnable = new Runnable() {
				public void run() {
					poolServer.serve();
				}
			};
			Thread thread = new Thread(runnable);
			thread.start();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back receicer: cannot start postback server because {}", e.getMessage());
		}
	}

	public void closeServer() {
		if (TsfileDBDescriptor.getInstance().getConfig().IS_POSTBACK_ENABLE) {
			poolServer.stop();
			serverTransport.close();
			LOGGER.info("Stop postback server!");
		}
	}
}