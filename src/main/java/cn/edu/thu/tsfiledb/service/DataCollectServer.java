package cn.edu.thu.tsfiledb.service;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.conf.TsFileDBConstant;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

import cn.edu.thu.tsfiledb.service.rpc.thrift.TSDataCollectService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSDataCollectService.Processor;


/**
 * A server to handle data collect and return request from client.
 */
public class DataCollectServer extends AbstractServer{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataCollectServer.class);

    private Processor<TSDataCollectService.Iface> processor;
    private TSDataCollectServiceImpl impl;

    public DataCollectServer(){
    		isStart = false;
    }

	public synchronized void startServer() {
        if (isStart) {
            LOGGER.info("{}: data collect and return server has been already running now", TsFileDBConstant.GLOBAL_DB_NAME);
            return;
        }
        LOGGER.info("{}: start data collect and return server...", TsFileDBConstant.GLOBAL_DB_NAME);

        thread = new Thread(new DataCollectServerThread());
        thread.start();

        LOGGER.info("{}: start data collect and return server successfully, listening on port {}", TsFileDBConstant.GLOBAL_DB_NAME,TsfileDBDescriptor.getInstance().getConfig().dataCollectPort);
        isStart = true;
	}

	public synchronized void restartServer() {
		stopServer();
		startServer();
	}

	public synchronized void stopServer() {
		if (!isStart) {
            LOGGER.info("{}: data collect server isn't running now", TsFileDBConstant.GLOBAL_DB_NAME);
            return;
        }
        LOGGER.info("{}: closing data collect server...", TsFileDBConstant.GLOBAL_DB_NAME);
        close();
        LOGGER.info("{}: close data collect server successfully", TsFileDBConstant.GLOBAL_DB_NAME);
	}
	
	class DataCollectServerThread implements Runnable{
		public DataCollectServerThread() {
            protocolFactory = new TBinaryProtocol.Factory();
            impl = new TSDataCollectServiceImpl();
            processor = new TSDataCollectService.Processor<TSDataCollectService.Iface>(impl);
		}
		
		@Override
		public void run() {
            try {
				serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().dataCollectPort);
				poolArgs = new TThreadPoolServer.Args(serverTransport);
				poolArgs.processor(processor);
				poolArgs.protocolFactory(protocolFactory);
				poolServer = new TThreadPoolServer(poolArgs);
				poolServer.serve();
            } catch (TTransportException e) {
                LOGGER.error("{}: failed to start data collect and return server, because ", TsFileDBConstant.GLOBAL_DB_NAME, e);
            } catch (Exception e) {
                LOGGER.error("{}: data collect and return server exit, because ", TsFileDBConstant.GLOBAL_DB_NAME, e);
            } finally {
                close();
                LOGGER.info("{}: close TThreadPoolServer and TServerSocket for data collect and return server", TsFileDBConstant.GLOBAL_DB_NAME);
            }
		}
	}
	
	public static void main(String[] args) {
		DataCollectServer server = new DataCollectServer();
		server.startServer();
	}
}
