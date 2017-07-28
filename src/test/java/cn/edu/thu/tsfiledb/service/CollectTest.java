package cn.edu.thu.tsfiledb.service;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeProcessor;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeProcessorStore;
import cn.edu.thu.tsfiledb.engine.filenode.IntervalFileNode;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFileNodeNameResp;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;

public class CollectTest {

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
	private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

	private FileNodeManager fManager = null;

	private String deltaObjectId = "root.vehicle.d0";
	private String deltaObjectId1 = "root.vehicle.d1";
	private String deltaObjectId2 = "root.vehicle.d2";
	private String measurementId = "s0";
	private String measurementId6 = "s6";
	private TSDataType dataType = TSDataType.INT32;

	private String FileNodeDir;
	private String BufferWriteDir;
	private String overflowDataDir;
	private int rowGroupSize;
	private int pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
	private int defaultMaxStringLength = tsconfig.maxStringLength;
	private boolean cachePageData = tsconfig.duplicateIncompletedPage;
	private int pageSize = tsconfig.pageSizeInByte;

	@Before
	public void setUp() throws Exception {

		FileNodeDir = tsdbconfig.fileNodeDir;
		BufferWriteDir = tsdbconfig.bufferWriteDir;
		overflowDataDir = tsdbconfig.overflowDataDir;

		tsdbconfig.fileNodeDir = "filenode" + File.separatorChar;
		tsdbconfig.bufferWriteDir = "bufferwrite";
		tsdbconfig.overflowDataDir = "overflow";
		tsdbconfig.metadataDir = "metadata";
		tsconfig.duplicateIncompletedPage = true;

		// set rowgroupsize
		tsconfig.groupSizeInByte = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSizeInByte = 100;
		tsconfig.maxStringLength = 2;
		EngineTestHelper.delete(tsdbconfig.fileNodeDir);
		EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.walFolder);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		MetadataManagerHelper.initMetadata();
		WriteLogManager.getInstance().close();

	}

	@After
	public void tearDown() throws Exception {
		WriteLogManager.getInstance().close();
		MManager.getInstance().flushObjectToFile();
		EngineTestHelper.delete(tsdbconfig.fileNodeDir);
		EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.walFolder);
		EngineTestHelper.delete(tsdbconfig.metadataDir);

		tsdbconfig.fileNodeDir = FileNodeDir;
		tsdbconfig.overflowDataDir = overflowDataDir;
		tsdbconfig.bufferWriteDir = BufferWriteDir;

		tsconfig.groupSizeInByte = rowGroupSize;
		tsconfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
		tsconfig.pageSizeInByte = pageSize;
		tsconfig.maxStringLength = defaultMaxStringLength;
		tsconfig.duplicateIncompletedPage = cachePageData;

	}

	@Test
	public void test() throws Exception {

		// set root.vehicle
		MetadataManagerHelper.initMetadata2();
		// create bufferwrite data
		// deltaObjectId0
		List<Pair<Long, Long>> d1TimeList = new ArrayList<>();
		d1TimeList.add(new Pair<Long, Long>(100L, 200L));
		d1TimeList.add(new Pair<Long, Long>(250L, 300L));
		d1TimeList.add(new Pair<Long, Long>(400L, 500L));
		createBufferwriteFiles(d1TimeList, deltaObjectId);
		// deltaObjectId1
		List<Pair<Long, Long>> d2TimeList = new ArrayList<>();
		d2TimeList.add(new Pair<Long, Long>(50L, 120L));
		d2TimeList.add(new Pair<Long, Long>(150L, 200L));
		d2TimeList.add(new Pair<Long, Long>(250L, 500L));
		d2TimeList.add(new Pair<Long, Long>(600L, 1000L));
		createBufferwriteFiles(d2TimeList, deltaObjectId1);
		// deltaObjectId2
		List<Pair<Long, Long>> d3TimeList = new ArrayList<>();
		d3TimeList.add(new Pair<Long, Long>(1000L, 2000L));
		d3TimeList.add(new Pair<Long, Long>(3000L, 4000L));
		d3TimeList.add(new Pair<Long, Long>(4500L, 5000L));
		d3TimeList.add(new Pair<Long, Long>(6000L, 10000L));
		createBufferwriteFiles(d3TimeList, deltaObjectId2);

		// query for collection
		fManager = FileNodeManager.getInstance();
		String nsp = null;
		try {
			List<String> fileNodeNames = MManager.getInstance().getAllFileNames();
			assertEquals(1, fileNodeNames.size());
			assertEquals("root.vehicle", fileNodeNames.get(0));
			nsp = fileNodeNames.get(0);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			int token = fManager.beginQuery(nsp);
			assertEquals(0, token);
			List<IntervalFileNode> files = fManager.collectQuery(nsp, new HashMap<>(), 200);
			assertEquals(3, files.size());
			IntervalFileNode temp = files.get(0);
			assertEquals(100, temp.getStartTime(deltaObjectId));
			assertEquals(200, temp.getEndTime(deltaObjectId));
			temp = files.get(1);
			assertEquals(50, temp.getStartTime(deltaObjectId1));
			assertEquals(120, temp.getEndTime(deltaObjectId1));
			temp = files.get(2);
			assertEquals(150, temp.getStartTime(deltaObjectId1));
			assertEquals(200, temp.getEndTime(deltaObjectId1));

			// for next qury startTime map
			Map<String, Long> startTimes = new HashMap<>();
			startTimes.put(deltaObjectId, 200L);
			startTimes.put(deltaObjectId1, 200L);
			int token2 = fManager.beginQuery(nsp);
			assertEquals(1, token2);
			fManager.endQuery(nsp, token);
			// for next query
			files = fManager.collectQuery(nsp, startTimes, 4000L);
			assertEquals(6, files.size());

			temp = files.get(0);
			assertEquals(250, temp.getStartTime(deltaObjectId));
			assertEquals(300, temp.getEndTime(deltaObjectId));
			temp = files.get(1);
			assertEquals(400, temp.getStartTime(deltaObjectId));
			assertEquals(500, temp.getEndTime(deltaObjectId));

			temp = files.get(2);
			assertEquals(250, temp.getStartTime(deltaObjectId1));
			assertEquals(500, temp.getEndTime(deltaObjectId1));
			temp = files.get(3);
			assertEquals(600, temp.getStartTime(deltaObjectId1));
			assertEquals(1000, temp.getEndTime(deltaObjectId1));

			temp = files.get(4);
			assertEquals(1000, temp.getStartTime(deltaObjectId2));
			assertEquals(2000, temp.getEndTime(deltaObjectId2));
			temp = files.get(5);
			assertEquals(3000, temp.getStartTime(deltaObjectId2));
			assertEquals(4000, temp.getEndTime(deltaObjectId2));

			fManager.endQuery(nsp, token2);

			fManager.closeAll();

		} catch (FileNodeManagerException e) {
			e.printStackTrace();
		}

		TSDataCollectServiceImpl collectServiceImpl = new TSDataCollectServiceImpl();
		System.out.println(collectServiceImpl.getAllFileNodeName());
		TSFileNodeNameResp resp = collectServiceImpl.getFileNode(new TSFileNodeNameReq(nsp, new HashMap<>(), 200));
		System.out.println(resp.getFileInfoList().size());
		System.out.println(resp.getToken());
	}

	private void createBufferwriteFiles(List<Pair<Long, Long>> pairList, String deltaObjectId) {
		for (Pair<Long, Long> timePair : pairList) {
			createBufferwriteFile(timePair, deltaObjectId);
		}
	}

	private void createBufferwriteFile(Pair<Long, Long> timePair, String deltaObjectId) {

		long startTime = timePair.left;
		long endTime = timePair.right;
		// create bufferwrite file
		fManager = FileNodeManager.getInstance();
		for (long i = startTime; i <= endTime; i++) {
			TSRecord record = new TSRecord(i, deltaObjectId);
			DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i));
			record.addTuple(dataPoint);
			try {
				fManager.insert(record);
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		try {
			// close
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
