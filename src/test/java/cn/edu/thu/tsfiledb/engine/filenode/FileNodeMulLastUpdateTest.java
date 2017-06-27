package cn.edu.thu.tsfiledb.engine.filenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;

public class FileNodeMulLastUpdateTest {

	private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();

	private FileNodeProcessor processor = null;

	private String deltaObjectId0 = "root.vehicle.d0";

	private String deltaObjectId2 = "root.vehicle.d2";

	private String deltaObjectId1 = "root.vehicle.d1";

	private String measurementId = "s0";

	private Map<String, Object> parameters = null;

	private FileNodeProcessorStore fileNodeProcessorStore;

	private String nameSpacePath = null;

	private Action overflowBackUpAction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("Manager overflow backup");
		}
	};

	private Action overflowFlushAction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("Manager overflow flush");
		}
	};

	@Before
	public void setUp() throws Exception {

		tsdbconfig.FileNodeDir = "filenode" + File.separatorChar;
		tsdbconfig.BufferWriteDir = "bufferwrite";
		tsdbconfig.overflowDataDir = "overflow";
		tsdbconfig.metadataDir = "metadata";
		// set rowgroupsize
		tsconfig.rowGroupSize = 10000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSize = 100;
		tsconfig.defaultMaxStringLength = 2;
		tsconfig.cachePageData = true;

		parameters = new HashMap<>();
		parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
		EngineTestHelper.delete(tsdbconfig.FileNodeDir);
		EngineTestHelper.delete(tsdbconfig.BufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		MetadataManagerHelper.initMetadata2();

		nameSpacePath = MManager.getInstance().getFileNameByPath(deltaObjectId0);
	}

	@After
	public void tearDown() throws Exception {

		EngineTestHelper.delete(tsdbconfig.FileNodeDir);
		EngineTestHelper.delete(tsdbconfig.BufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		MetadataManagerHelper.clearMetadata();

	}

	@Test
	public void WriteCloseAndQueryTest() {

		// write file
		// file1: d0[10,20]
		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferWrite();
		// file2: d1[10,20]
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferWrite();
		// file3: d2[10,20]
		createBufferwriteFile(10, 20, deltaObjectId2);
		closeBufferWrite();
		// file4: d0,d1 [30,40]
		createBufferwriteFile(30, 40, deltaObjectId0, deltaObjectId1);
		closeBufferWrite();
		// file5: d0,d1,d2 [50,60]
		createBufferwriteFile(50, 60, deltaObjectId0, deltaObjectId1, deltaObjectId2);
		closeBufferWrite();
		// file6: d0,d1,d2 [70,80....
		createBufferwriteFile(70, 80, deltaObjectId0, deltaObjectId1, deltaObjectId2);

		// query
		try {
			// add overflow
			processor.getOverflowProcessor(nameSpacePath, parameters);
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);

			DynamicOneColumnData CachePage = queryStructure.getCurrentPage();
			Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList = queryStructure.getPageList();
			List<RowGroupMetaData> bufferwriteDataInDisk = queryStructure.getBufferwriteDataInDisk();
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			List<Object> allOverflowData = queryStructure.getAllOverflowData();

			// check overflow
			assertEquals(4, allOverflowData.size());
			assertEquals(null, allOverflowData.get(0));
			assertEquals(null, allOverflowData.get(1));
			assertEquals(null, allOverflowData.get(2));
			assertEquals(null, allOverflowData.get(3));

			// check memory data
			if (CachePage != null) {
				for (ByteArrayInputStream stream : pageList.left) {
					DynamicOneColumnData pagedata = PageTestUtils.pageToDynamic(stream, pageList.right, deltaObjectId0,
							measurementId);
					CachePage.mergeRecord(pagedata);
				}
				assertEquals(11, CachePage.length);
				assertEquals(0, bufferwriteDataInDisk.size());
			} else {
				assertEquals(1, bufferwriteDataInDisk.size());
			}
			/**
			 * check file
			 */
			assertEquals(6, bufferwriteDataInFiles.size());
			// first file
			IntervalFileNode temp = bufferwriteDataInFiles.get(0);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId0, d);
				assertEquals(10, temp.getStartTime(deltaObjectId0));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId0, d);
				assertEquals(20, temp.getEndTime(deltaObjectId0));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// second file
			temp = bufferwriteDataInFiles.get(1);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId1, d);
				assertEquals(10, temp.getStartTime(deltaObjectId1));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId1, d);
				assertEquals(20, temp.getEndTime(deltaObjectId1));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// third file
			temp = bufferwriteDataInFiles.get(2);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId2, d);
				assertEquals(10, temp.getStartTime(deltaObjectId2));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId2, d);
				assertEquals(20, temp.getEndTime(deltaObjectId2));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// fourth file
			temp = bufferwriteDataInFiles.get(3);
			assertEquals(2, temp.getStartTimeMap().size());
			assertEquals(2, temp.getEndTimeMap().size());
			assertEquals(30, temp.getStartTime(deltaObjectId0));
			assertEquals(30, temp.getStartTime(deltaObjectId1));
			assertEquals(40, temp.getEndTime(deltaObjectId0));
			assertEquals(40, temp.getEndTime(deltaObjectId1));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// fifth file
			temp = bufferwriteDataInFiles.get(4);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(50, temp.getStartTime(deltaObjectId0));
			assertEquals(50, temp.getStartTime(deltaObjectId1));
			assertEquals(50, temp.getStartTime(deltaObjectId2));
			assertEquals(60, temp.getEndTime(deltaObjectId0));
			assertEquals(60, temp.getEndTime(deltaObjectId1));
			assertEquals(60, temp.getEndTime(deltaObjectId2));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// sixth
			temp = bufferwriteDataInFiles.get(5);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(0, temp.getEndTimeMap().size());
			assertEquals(70, temp.getStartTime(deltaObjectId0));
			assertEquals(70, temp.getStartTime(deltaObjectId1));
			assertEquals(70, temp.getStartTime(deltaObjectId2));
			assertEquals(-1, temp.getEndTime(deltaObjectId0));
			assertEquals(-1, temp.getEndTime(deltaObjectId1));
			assertEquals(-1, temp.getEndTime(deltaObjectId2));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void WriteOverflowAndQueryTest() {
		WriteCloseAndQueryTest();
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
			processor.getOverflowProcessor(nameSpacePath, parameters);
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			// test origin
			DynamicOneColumnData CachePage = queryStructure.getCurrentPage();
			Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList = queryStructure.getPageList();
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			List<Object> allOverflowData = queryStructure.getAllOverflowData();
			assertEquals(null, CachePage);
			assertEquals(null, pageList);
			IntervalFileNode temp = bufferwriteDataInFiles.get(5);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(70, temp.getStartTime(deltaObjectId0));
			assertEquals(70, temp.getStartTime(deltaObjectId1));
			assertEquals(70, temp.getStartTime(deltaObjectId2));
			assertEquals(80, temp.getEndTime(deltaObjectId0));
			assertEquals(80, temp.getEndTime(deltaObjectId1));
			assertEquals(80, temp.getEndTime(deltaObjectId2));

			// overflow data
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			// file 0
			ofProcessor.insert(deltaObjectId0, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId0, 5);
			// file 2
			ofProcessor.insert(deltaObjectId2, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId2, 5);
			// file 4
			ofProcessor.insert(deltaObjectId1, measurementId, 65, TSDataType.INT32, String.valueOf(65));
			processor.changeTypeToChanged(deltaObjectId1, 65);

			queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			allOverflowData = queryStructure.getAllOverflowData();
			DynamicOneColumnData insert = (DynamicOneColumnData) allOverflowData.get(0);
			assertEquals(1, insert.length);
			assertEquals(5, insert.getTime(0));
			assertEquals(5, insert.getInt(0));

			temp = bufferwriteDataInFiles.get(0);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);

			temp = bufferwriteDataInFiles.get(2);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);

			temp = bufferwriteDataInFiles.get(4);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);

			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void WriteEmptyAndMergeTest() {
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
			processor.setLastUpdateTime(deltaObjectId0, 100);
			processor.setLastUpdateTime(deltaObjectId1, 100);
			processor.setLastUpdateTime(deltaObjectId2, 100);

			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			ofProcessor.insert(deltaObjectId0, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId0, 5);
			ofProcessor.insert(deltaObjectId0, measurementId, 10, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId0, 10);
			ofProcessor.insert(deltaObjectId1, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId1, 5);

			processor.writeLock();
			processor.merge();

			// this can add one thread which add lock and bufferwrite data
			// add bufferwrite data and add overflow

			// or add overflow data

			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			DynamicOneColumnData CachePage = queryStructure.getCurrentPage();
			Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList = queryStructure.getPageList();
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			List<Object> allOverflowData = queryStructure.getAllOverflowData();

			assertEquals(null, CachePage);
			assertEquals(null, pageList);
			assertEquals(null, allOverflowData.get(0));

			assertEquals(1, bufferwriteDataInFiles.size());
			IntervalFileNode temp = bufferwriteDataInFiles.get(0);
			assertEquals(2, temp.getStartTimeMap().size());
			assertEquals(5, temp.getStartTime(deltaObjectId0));
			assertEquals(10, temp.getEndTime(deltaObjectId0));
			assertEquals(5, temp.getStartTime(deltaObjectId1));
			assertEquals(5, temp.getEndTime(deltaObjectId1));

			processor.close();

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void WriteEmptyAndFileMergeTest() {
		// add overflow
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
			processor.setLastUpdateTime(deltaObjectId0, 100);
			processor.setLastUpdateTime(deltaObjectId1, 100);
			processor.setLastUpdateTime(deltaObjectId2, 100);
			// add overflow d0 d1
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			ofProcessor.insert(deltaObjectId0, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId0, 5);
			ofProcessor.insert(deltaObjectId0, measurementId, 10, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId0, 10);
			ofProcessor.insert(deltaObjectId1, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId1, 5);
			// add bufferwrite
			processor.close();

			createBufferwriteFile(150, 200, deltaObjectId2);
			closeBufferWrite();
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
			ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			processor.writeLock();
			//
			// add thread to lock and query
			//
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						processor.writeLock();
						QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null,
								null);
						assertEquals(null, queryStructure.getPageList());
						assertEquals(null, queryStructure.getCurrentPage());
						assertEquals(0, queryStructure.getBufferwriteDataInDisk().size());
						assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());
						IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);

						// assert status
						assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
						assertEquals(3, temp.getStartTimeMap().size());
						assertEquals(0, temp.getStartTime(deltaObjectId0));
						assertEquals(10, temp.getEndTime(deltaObjectId0));
						assertEquals(0, temp.getStartTime(deltaObjectId1));
						assertEquals(5, temp.getEndTime(deltaObjectId1));
						assertEquals(150, temp.getStartTime(deltaObjectId2));
						assertEquals(200, temp.getEndTime(deltaObjectId2));
					} catch (FileNodeProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						processor.writeUnlock();
					}

				}
			});
			thread.start();
			Thread.sleep(1000);
			processor.merge();
			// assert status after merge
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(null, queryStructure.getPageList());
			assertEquals(null, queryStructure.getCurrentPage());
			assertEquals(0, queryStructure.getBufferwriteDataInDisk().size());
			assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());

			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(0, temp.getStartTime(deltaObjectId0));
			assertEquals(10, temp.getEndTime(deltaObjectId0));
			assertEquals(0, temp.getStartTime(deltaObjectId1));
			assertEquals(5, temp.getEndTime(deltaObjectId1));
			assertEquals(150, temp.getStartTime(deltaObjectId2));
			assertEquals(200, temp.getEndTime(deltaObjectId2));

			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void WriteFileDeleteAndMergeOverflow() {
		// write tsfile
		WriteCloseAndQueryTest();
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// delete overflow
		try {
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			ofProcessor.delete(deltaObjectId2, measurementId, 25, TSDataType.INT32);
			processor.changeTypeToChangedForDelete(deltaObjectId2, 25);
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge and overflow
		processor.writeLock();
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					processor.writeLock();
					// query
					QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
					assertEquals(null, queryStructure.getPageList());
					assertEquals(null, queryStructure.getCurrentPage());
					assertEquals(0, queryStructure.getBufferwriteDataInDisk().size());
					assertEquals(6, queryStructure.getBufferwriteDataInFiles().size());
					assertEquals(OverflowChangeType.CHANGED, queryStructure.getBufferwriteDataInFiles().get(2));

					// add overflow
					OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);

					ofProcessor.insert(deltaObjectId2, measurementId, 15, TSDataType.INT32, String.valueOf(15));
					processor.changeTypeToChanged(deltaObjectId2, 15);
					queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
					assertEquals(OverflowChangeType.MERGING_CHANGE, queryStructure.getBufferwriteDataInFiles().get(2));
				} catch (FileNodeProcessorException | OverflowProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} finally {
					processor.writeUnlock();
				}
			}
		});
		thread.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			processor.merge();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// assert query
		try {
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(5, queryStructure.getBufferwriteDataInFiles().size());
			assertEquals(OverflowChangeType.CHANGED, queryStructure.getBufferwriteDataInFiles().get(3));
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge and assert query
		processor.writeLock();
		try {
			processor.merge();
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(5, queryStructure.getBufferwriteDataInFiles().size());
			assertEquals(OverflowChangeType.NO_CHANGE, queryStructure.getBufferwriteDataInFiles().get(3));
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(3);
			assertEquals(50, temp.getStartTime(deltaObjectId0));
			assertEquals(60, temp.getEndTime(deltaObjectId0));
			assertEquals(50, temp.getStartTime(deltaObjectId1));
			assertEquals(60, temp.getEndTime(deltaObjectId1));
			assertEquals(15, temp.getStartTime(deltaObjectId2));
			assertEquals(60, temp.getEndTime(deltaObjectId2));
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void FileEmptyMergeAnaWrite() {

		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferWrite();
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferWrite();
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
			// deltaObjectId2 empty overflow
			processor.setLastUpdateTime(deltaObjectId2, 100);
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			ofProcessor.insert(deltaObjectId2, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId2, 5);

			processor.writeLock();
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {

					try {
						processor.writeLock();
						QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null,
								null);
						assertEquals(2, queryStructure.getBufferwriteDataInFiles().size());
						IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
						assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
						assertEquals(10, temp.getStartTime(deltaObjectId0));
						assertEquals(20, temp.getEndTime(deltaObjectId0));
						assertEquals(0, temp.getStartTime(deltaObjectId2));
						assertEquals(100, temp.getEndTime(deltaObjectId2));

						// write bufferwrite data
						// file 2: [200,400...)
						BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, 200);
						bfProcessor.write(deltaObjectId2, measurementId, 200, TSDataType.INT32, String.valueOf(200));
						processor.addIntervalFileNode(200, bfProcessor.getFileAbsolutePath());
						processor.setIntervalFileNodeStartTime(deltaObjectId2, 200);
						processor.setLastUpdateTime(deltaObjectId2, 200);
						bfProcessor.write(deltaObjectId2, measurementId, 400, TSDataType.INT32, String.valueOf(400));
						processor.setLastUpdateTime(deltaObjectId2, 400);
						// add overflow
						queryStructure = processor.query(deltaObjectId2, measurementId, null, null, null);
						assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
						temp = queryStructure.getBufferwriteDataInFiles().get(2);
						assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
						assertEquals(200, temp.getStartTime(deltaObjectId2));
						assertEquals(-1, temp.getEndTime(deltaObjectId2));
						OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
						ofProcessor.insert(deltaObjectId2, measurementId, 100, TSDataType.INT32, String.valueOf(100));
						processor.changeTypeToChanged(deltaObjectId2, 100);
						// merge changed
						queryStructure = processor.query(deltaObjectId2, measurementId, null, null, null);
						assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
						temp = queryStructure.getBufferwriteDataInFiles().get(2);
						assertEquals(OverflowChangeType.MERGING_CHANGE, temp.overflowChangeType);

					} catch (FileNodeProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} catch (BufferWriteProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} catch (OverflowProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						processor.writeUnlock();
					}
				}
			});

			thread.start();
			Thread.sleep(1000);
			processor.merge();

			//
			// query data
			//
			QueryStructure queryStructure = processor.query(deltaObjectId2, measurementId, null, null, null);
			assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(1);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(10, temp.getStartTime(deltaObjectId1));
			assertEquals(20, temp.getEndTime(deltaObjectId1));
			temp = queryStructure.getBufferwriteDataInFiles().get(2);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(200, temp.getStartTime(deltaObjectId2));
			assertEquals(-1, temp.getEndTime(deltaObjectId2));

			temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(10, temp.getStartTime(deltaObjectId0));
			assertEquals(20, temp.getEndTime(deltaObjectId0));
			assertEquals(5, temp.getStartTime(deltaObjectId2));
			assertEquals(5, temp.getEndTime(deltaObjectId2));

			processor.close();

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	/**
	 * Write bufferwrite data and close
	 * 
	 * @param begin
	 * @param end
	 * @param deltaObjectId
	 */
	private void createBufferwriteFile(long begin, long end, String... deltaObjectIds) {

		for (int i = 0; i < deltaObjectIds.length; i++) {
			String deltaObjectId = deltaObjectIds[i];
			if (i == 0) {
				try {
					processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, nameSpacePath, parameters);
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, begin);
					assertEquals(true, bfProcessor.isNewProcessor());
					bfProcessor.write(deltaObjectId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
					bfProcessor.setNewProcessor(false);
					processor.addIntervalFileNode(begin, bfProcessor.getFileAbsolutePath());
					processor.setIntervalFileNodeStartTime(deltaObjectId, begin);
					processor.setLastUpdateTime(deltaObjectId, begin);
				} catch (FileNodeProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			} else {
				try {
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, begin);
					bfProcessor.write(deltaObjectId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
					processor.setIntervalFileNodeStartTime(deltaObjectId, begin);
					processor.setLastUpdateTime(deltaObjectId, begin);
				} catch (FileNodeProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		}

		for (long i = begin + 1; i <= end; i++) {
			for (String deltaObjectId : deltaObjectIds) {
				try {
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, i);
					bfProcessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
					processor.setIntervalFileNodeStartTime(deltaObjectId, i);
					processor.setLastUpdateTime(deltaObjectId, i);
				} catch (FileNodeProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		}
	}

	/**
	 * Close the filenode processor
	 */
	private void closeBufferWrite() {

		try {
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
