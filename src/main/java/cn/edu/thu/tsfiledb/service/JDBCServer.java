package cn.edu.thu.tsfiledb.service;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService.Processor;

/**
 * A server to handle jdbc request from client.
 */
public class JDBCServer extends AbstractServer implements JDBCServerMBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCServer.class);

    private Processor<TSIService.Iface> processor;
    private TSServiceImpl impl;

    public JDBCServer() {
        isStart = false;
    }

    @Override
    public synchronized void startServer() {
        if (isStart) {
            LOGGER.info("TsFileDB: jdbc server has been already running now");
            return;
        }
        LOGGER.info("TsFileDB: start jdbc server...");

        try {
            thread = new Thread(new JDBCServerThread());
        } catch (IOException e) {
            LOGGER.error("TsFileDB: failed to start jdbc server. {}", e.getMessage());
            return;
        }
        thread.start();

        LOGGER.info("TsFileDB: start jdbc server successfully, listening on port {}",TsfileDBDescriptor.getInstance().getConfig().rpcPort);
        isStart = true;
    }

    @Override
    public synchronized void restartServer() {
        stopServer();
        startServer();
    }

    @Override
    public synchronized void stopServer() {
        if (!isStart) {
            LOGGER.info("TsFileDB: jdbc server isn't running now");
            return;

        }
        LOGGER.info("TsFileDB: closing jdbc server...");
        close();
        LOGGER.info("TsFileDB: close jdbc server successfully");
    }

    private class JDBCServerThread implements Runnable {

        public JDBCServerThread() throws IOException {
            protocolFactory = new TBinaryProtocol.Factory();
            impl = new TSServiceImpl();
            processor = new TSIService.Processor<TSIService.Iface>(impl);
        }

        @Override
        public void run() {
            try {
				serverTransport = new TServerSocket(TsfileDBDescriptor.getInstance().getConfig().rpcPort);
				poolArgs = new TThreadPoolServer.Args(serverTransport);
				poolArgs.processor(processor);
				poolArgs.protocolFactory(protocolFactory);
				poolServer = new TThreadPoolServer(poolArgs);
				poolServer.setServerEventHandler(new JDBCServerEventHandler(impl));
				poolServer.serve();
            } catch (TTransportException e) {
                LOGGER.error("TsFileDB: failed to start jdbc server, because ", e);
            } catch (Exception e) {
                LOGGER.error("TsFileDB: jdbc server exit, because ", e);
            } finally {
                close();
                LOGGER.info("TsFileDB: close TThreadPoolServer and TServerSocket for jdbc server");
            }
        }
    }

    public static void main(String[] args) throws TTransportException, InterruptedException {
        JDBCServer server = new JDBCServer();
        server.startServer();
    }
}
