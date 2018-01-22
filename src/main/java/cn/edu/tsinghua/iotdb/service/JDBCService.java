package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.concurrent.ThreadName;
import cn.edu.tsinghua.iotdb.conf.TsFileConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService.Processor;

/**
 * A service to handle jdbc request from client.
 */
public class JDBCService implements JDBCServiceMBean, IService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCService.class);
    private Thread jdbcServiceThread;
    private boolean isStart;

    private Factory protocolFactory;
    private Processor<TSIService.Iface> processor;
    private TServerSocket serverTransport;
    private TThreadPoolServer.Args poolArgs;
    private TServer poolServer;
    private TSServiceImpl impl;
    
    private final String STATUS_UP = "UP";
    private final String STATUS_DOWN = "DOWN";
    
    private final String MBEAN_NAME = String.format("%s:%s=%s", TsFileConstant.IOTDB_PACKAGE, TsFileConstant.JMX_TYPE, getID().getJmxName());
    
	private static class JDBCServiceHolder{
		private static final JDBCService INSTANCE = new JDBCService();
	}
	
	public static final JDBCService getInstance() {
		return JDBCServiceHolder.INSTANCE;
	}    
	
    private JDBCService() {
        isStart = false;
    }
    
	@Override
	public String getJDBCServiceStatus() {
		return isStart ? STATUS_UP : STATUS_DOWN;
	}
	
	@Override
	public int getRPCPort() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		return config.rpcPort;
	}
	
	@Override
	public void start() {
		JMXService.registerMBean(getInstance(), MBEAN_NAME);
		startService();
	}

	@Override
	public void stop() {
		stopService();
		JMXService.deregisterMBean(MBEAN_NAME);
	}

	@Override
	public ServiceType getID() {
		return ServiceType.JDBC_SERVICE;
	}
    
    @Override
    public synchronized void startService() {
        if (isStart) {
            LOGGER.info("{}: {} has been already running now", TsFileConstant.GLOBAL_DB_NAME, this.getID().getName());
            return;
        }
        LOGGER.info("{}: start {}...",TsFileConstant.GLOBAL_DB_NAME, this.getID().getName());

        try {
        	jdbcServiceThread = new Thread(new JDBCServiceThread());
        	jdbcServiceThread.setName(ThreadName.JDBC_SERVICE.getName());
        } catch (IOException e) {
            LOGGER.error("{}: failed to start {} because of {}",
            		TsFileConstant.GLOBAL_DB_NAME, 
            		this.getID().getName(),
            		e.getMessage());
            return;
        }
        jdbcServiceThread.start();

        LOGGER.info("{}: start {} successfully, listening on port {}",
        		TsFileConstant.GLOBAL_DB_NAME, 
        		this.getID().getName(), 
        		TsfileDBDescriptor.getInstance().getConfig().rpcPort);
        isStart = true;
    }

    @Override
    public synchronized void restartService() {
        stopService();
        startService();
    }

    @Override
    public synchronized void stopService() {
        if (!isStart) {
            LOGGER.info("{}: {} isn't running now",TsFileConstant.GLOBAL_DB_NAME, this.getID().getName());
            return;
        }
        LOGGER.info("{}: closing {}...", TsFileConstant.GLOBAL_DB_NAME, this.getID().getName());
        close();
        LOGGER.info("{}: close {} successfully", TsFileConstant.GLOBAL_DB_NAME, this.getID().getName());
    }

    private synchronized void close() {
    	if (poolServer != null) {
            poolServer.stop();
            poolServer = null;
        }

        if (serverTransport != null) {
            serverTransport.close();
            serverTransport = null;
        }
        isStart = false;
    }

    private class JDBCServiceThread implements Runnable {

        public JDBCServiceThread() throws IOException {
            protocolFactory = new TBinaryProtocol.Factory();
            impl = new TSServiceImpl();
            processor = new TSIService.Processor<TSIService.Iface>(impl);
        }

        @Override
        public void run() {
            try {
                serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().rpcPort);
                poolArgs = new TThreadPoolServer.Args(serverTransport);
                poolArgs.executorService = IoTDBThreadPoolFactory.createJDBCClientThreadPool(poolArgs, ThreadName.JDBC_CLIENT.getName());
                poolArgs.processor(processor);
                poolArgs.protocolFactory(protocolFactory);
                poolServer = new TThreadPoolServer(poolArgs);
                poolServer.setServerEventHandler(new JDBCServiceEventHandler(impl));
                poolServer.serve();
            } catch (TTransportException e) {
                LOGGER.error("{}: failed to start {}, because ",TsFileConstant.GLOBAL_DB_NAME, getID().getName(), e);
            } catch (Exception e) {
                LOGGER.error("{}: {} exit, because ",TsFileConstant.GLOBAL_DB_NAME, getID().getName(), e);
            } finally {
                close();
                LOGGER.info("{}: close TThreadPoolServer and TServerSocket for {}",TsFileConstant.GLOBAL_DB_NAME, getID().getName());
            }
        }
    }

    public static void main(String[] args) {
        JDBCService server = JDBCService.getInstance();
        server.start();
    }
}
