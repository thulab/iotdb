package cn.edu.thu.tsfiledb.service;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

public abstract class AbstractServer {
	protected boolean isStart;

	protected TProtocolFactory protocolFactory;
    
	protected TServerSocket serverTransport;
    protected TThreadPoolServer.Args poolArgs;
    protected TServer poolServer;  
    protected Thread thread;
    
    protected synchronized void close() {
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
}
