package cn.edu.thu.tsfiledb.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import cn.edu.thu.tsfiledb.service.rpc.thrift.TSBackFileNodeReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSBackFileNodeResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSDataCollectService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileInfo;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameAllResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;

public class TSDataCollectServiceImpl implements TSDataCollectService.Iface{

	@Override
	public TSFileNodeNameAllResp getAllFileNodeName() throws TException {
		TSFileNodeNameAllResp resp = new TSFileNodeNameAllResp(new ArrayList<String>());
		return resp;
	}

	@Override
	public TSFileNodeNameResp getFileNode(TSFileNodeNameReq req) throws TException {
		String nameSpacePath = req.getNameSpacePath();
		Map<String, Long> startTimes = req.getStartTimes();
		long endTime = req.getEndTime();
		
		int token = 123;
		List<TSFileInfo> fileInfoList = new ArrayList<>();
		TSFileNodeNameResp resp = new TSFileNodeNameResp(fileInfoList, token);		
		return resp;
	}

	@Override
	public TSBackFileNodeResp backFileNode(TSBackFileNodeReq req) throws TException {
		String nameSpacePath = req.getNameSpacePath();
		List<TSFileInfo> fileInfoList = req.getFileInfoList();
		int token = req.getToken();
		
		TSBackFileNodeResp resp = new TSBackFileNodeResp(true);
		return resp;
	}

}
