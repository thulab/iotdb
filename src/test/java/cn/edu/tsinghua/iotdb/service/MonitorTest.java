package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.*;
import cn.edu.tsinghua.iotdb.engine.lru.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.overflow.io.EngineTestHelper;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author liukun
 *
 */
public class MonitorTest {

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
	private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

	private FileNodeManager fManager = null;

	private String deltaObjectId = "root.vehicle.d0";
	private String deltaObjectId2 = "root.vehicle.d1";
	private String measurementId = "s0";
	private String measurementId6 = "s6";
	private TSDataType dataType = TSDataType.INT32;

	private String FileNodeDir;
	private String BufferWriteDir;
	private int rowGroupSize;
	private int pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
	private int defaultMaxStringLength = tsconfig.maxStringLength;
	private boolean cachePageData = tsconfig.duplicateIncompletedPage;
	private int pageSize = tsconfig.pageSizeInByte;

	@Before
	public void setUp() throws Exception {
		FileNodeDir = tsdbconfig.fileNodeDir;
		BufferWriteDir = tsdbconfig.bufferWriteDir;
		tsdbconfig.fileNodeDir = "filenode" + File.separatorChar;
		tsdbconfig.bufferWriteDir = "bufferwrite";
		tsdbconfig.metadataDir = "metadata";
		tsconfig.duplicateIncompletedPage = true;

		// set rowgroupsize
		tsconfig.groupSizeInByte = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSizeInByte = 100;
		tsconfig.maxStringLength = 2;
		EngineTestHelper.delete(tsdbconfig.fileNodeDir);
		EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
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
		EngineTestHelper.delete(tsdbconfig.walFolder);
		EngineTestHelper.delete(tsdbconfig.metadataDir);

		tsdbconfig.fileNodeDir = FileNodeDir;
		tsdbconfig.bufferWriteDir = BufferWriteDir;

		tsconfig.groupSizeInByte = rowGroupSize;
		tsconfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
		tsconfig.pageSizeInByte = pageSize;
		tsconfig.maxStringLength = defaultMaxStringLength;
		tsconfig.duplicateIncompletedPage = cachePageData;
	}

	@Test
	public void testBufferwriteAndAddMetadata() {
		createBufferwriteInMemory(new Pair<Long, Long>(1000L, 1001L), measurementId);
		fManager = FileNodeManager.getInstance();

		// add metadata
		MManager mManager = MManager.getInstance();
		assertEquals(false, mManager.pathExist(deltaObjectId + "." + measurementId6));
		try {
			mManager.addPathToMTree(deltaObjectId + "." + measurementId6, "INT32", "RLE", new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, mManager.pathExist(deltaObjectId + "." + measurementId6));
		// check level
		String nsp = null;
		try {
			nsp = mManager.getFileNameByPath(deltaObjectId + "." + measurementId6);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			fManager.closeOneFileNode(nsp);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		createBufferwriteInMemory(new Pair<Long, Long>(200L, 302L), measurementId6);
		// write data
		try {
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testBufferwriteAndAddMetadata2(){
		createBufferwriteInMemory(new Pair<Long, Long>(10L, 101L), measurementId);
		fManager = FileNodeManager.getInstance();

		// add metadata
		MManager mManager = MManager.getInstance();
		assertEquals(false, mManager.pathExist(deltaObjectId + "." + measurementId6));
		try {
			mManager.addPathToMTree(deltaObjectId + "." + measurementId6, "INT32", "RLE", new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, mManager.pathExist(deltaObjectId + "." + measurementId6));
		// check level
		String nsp = null;
		try {
			nsp = mManager.getFileNameByPath(deltaObjectId + "." + measurementId6);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			fManager.closeOneFileNode(nsp);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		createBufferwriteInMemory(new Pair<Long, Long>(200L, 302L), measurementId6);
		// write data
		try {
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBufferwriteInsert() {

		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);

		createBufferwriteInMemory(new Pair<Long, Long>(900L, 1000L), measurementId);

		fManager = FileNodeManager.getInstance();
		try {
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteinmemory = queryResult.getCurrentPage();
			List<RowGroupMetaData> bufferwriteinDisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(true, bufferwriteinmemory != null);
			assertEquals(true, bufferwriteinDisk != null);
			List<IntervalFileNode> newInterFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(5, newInterFiles.size());
			for (int i = 0; i < pairList.size(); i++) {
				IntervalFileNode temp = newInterFiles.get(i);
				Pair<Long, Long> time = pairList.get(i);
				assertEquals(time.left.longValue(), temp.getStartTime(deltaObjectId));
				assertEquals(time.right.longValue(), temp.getEndTime(deltaObjectId));
				System.out.println(time);
			}
			IntervalFileNode temp = newInterFiles.get(4);
			assertEquals(900, temp.getStartTime(deltaObjectId));
			assertEquals(-1, temp.getEndTime(deltaObjectId));

			List<Object> overflowResult = queryResult.getAllOverflowData();
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			fManager.endQuery(deltaObjectId, token);
			fManager.closeAll();
			// waitToSleep();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
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

	private void createBufferwriteInMemory(Pair<Long, Long> timePair, String measurementId) {
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
	}
}
