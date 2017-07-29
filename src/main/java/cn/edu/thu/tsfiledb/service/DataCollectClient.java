package cn.edu.thu.tsfiledb.service;

import cn.edu.thu.tsfiledb.service.rpc.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DataCollectClient {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataCollectClient.class);

    private String host;
    private int port;
    private int timeoutInMs;
    
    public DataCollectClient(String host, int port, int timeoutInMs){
    		this.host = host;
    		this.port = port;
    		this.timeoutInMs = timeoutInMs;
    }
    
    public DataCollectClient(String host, int port){
    		this(host, port, 10000);
    }
    
    public TSFileNodeNameAllResp getFileAllNode(){
    		TSocket transport = null;    		
    		TSDataCollectService.Iface client = null;
    		try {
    			transport = new TSocket(host, port, timeoutInMs);
    			try {
    				transport.getSocket().setKeepAlive(true);
    			} catch (SocketException e) {
    			    LOGGER.error("Cannot set socket keep alive", e);
    			}
    			if (!transport.isOpen()) {
    			    transport.open();
    			}
    			client = new TSDataCollectService.Client(new TBinaryProtocol(transport));
        		TSFileNodeNameAllResp resp = client.getAllFileNodeName();
        		return resp;
		} catch (Exception e) {
			LOGGER.error("Failed to get file all node.", e);
		} finally {
			if(transport != null){
				transport.close();
			}
		}
    		return null;
    }
    
	public TSFileNodeNameResp getFileNode(String nameSpacePath, Map<String, Long> startTimes, long endTime) {
		TSocket transport = null;
		TSDataCollectService.Iface client = null;
		try {
			transport = new TSocket(host, port, timeoutInMs);
			try {
				transport.getSocket().setKeepAlive(true);
			} catch (SocketException e) {
			    LOGGER.error("Cannot set socket keep alive", e);
			}
			if (!transport.isOpen()) {
			    transport.open();
			}
			client = new TSDataCollectService.Client(new TBinaryProtocol(transport));			
			TSFileNodeNameReq req = new TSFileNodeNameReq(nameSpacePath, startTimes, endTime);
			TSFileNodeNameResp resp = client.getFileNode(req);
			return resp;
		} catch (Exception e) {
			LOGGER.error("Failed to get file node.", e);
		} finally {
			if (transport != null) {
				transport.close();
			}
		}
		return null;
	}
    
	public TSBackFileNodeResp backFileNode(String nameSpacePath, List<TSFileInfo> fileInfos, int token) {
		TSocket transport = null;
		TSDataCollectService.Iface client = null;
		try {
			transport = new TSocket(host, port, timeoutInMs);
			try {
				transport.getSocket().setKeepAlive(true);
			} catch (SocketException e) {
			    LOGGER.error("Cannot set socket keep alive", e);
			}
			if (!transport.isOpen()) {
			    transport.open();
			}			client = new TSDataCollectService.Client(new TBinaryProtocol(transport));			
			TSBackFileNodeReq req = new TSBackFileNodeReq(nameSpacePath, fileInfos, token);
			TSBackFileNodeResp resp = client.backFileNode(req);
			return resp;
		} catch (Exception e) {
			LOGGER.error("Failed to back file node.", e);
		} finally {
			if (transport != null) {
				transport.close();
			}
		}
		return null;		
	}
	
    public static void main(String[] args) {
        DataCollectClient client = new DataCollectClient("127.0.0.1", 6668);
        System.out.println(client.getFileAllNode());
        System.out.println(client.getFileNode("123", new HashMap<>(), 1233L));
        System.out.println(client.backFileNode("123", new ArrayList<TSFileInfo>(), 13));
    }
}
