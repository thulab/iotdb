package cn.edu.thu.tsfiledb.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.engine.filenode.IntervalFileNode;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSBackFileNodeReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSBackFileNodeResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSDataCollectService;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileInfo;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameAllResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;

public class TSDataCollectServiceImpl implements TSDataCollectService.Iface {

	private MManager mManager = MManager.getInstance();
	private FileNodeManager fileNodeManager = FileNodeManager.getInstance();
	private int currentToken;

	@Override
	public TSFileNodeNameAllResp getAllFileNodeName() throws TException {
		TSFileNodeNameAllResp resp = new TSFileNodeNameAllResp(new ArrayList<String>());
		try {
			resp.setFileNodesList(mManager.getAllFileNames());
		} catch (PathErrorException e) {
			e.printStackTrace();
			// TODO
		}
		return resp;
	}

	@Override
	public TSFileNodeNameResp getFileNode(TSFileNodeNameReq req) throws TException {
		String nameSpacePath = req.getNameSpacePath();
		Map<String, Long> startTimes = req.getStartTimes();
		long endTime = req.getEndTime();
		List<IntervalFileNode> intervalFileNodes = null;
		try {
			this.currentToken = fileNodeManager.beginQuery(nameSpacePath);
			intervalFileNodes = fileNodeManager.collectQuery(nameSpacePath, startTimes, endTime);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			// TODO
		}
		List<TSFileInfo> fileInfoList = new ArrayList<>();
		for (IntervalFileNode intervalFileNode : intervalFileNodes) {
			TSFileInfo fileInfo = new TSFileInfo(intervalFileNode.filePath, intervalFileNode.getStartTimeMap(),
					intervalFileNode.getEndTimeMap());
			fileInfoList.add(fileInfo);
		}
		TSFileNodeNameResp resp = new TSFileNodeNameResp(fileInfoList, currentToken);
		return resp;
	}

	@Override
	public TSBackFileNodeResp backFileNode(TSBackFileNodeReq req) throws TException {
		String nameSpacePath = req.getNameSpacePath();
		List<TSFileInfo> fileInfoList = req.getFileInfoList();
		int token = req.getToken();
		try {
			fileNodeManager.endQuery(nameSpacePath, token);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			// TODO
		}
		TSBackFileNodeResp resp = new TSBackFileNodeResp(true);
		return resp;
	}

}
