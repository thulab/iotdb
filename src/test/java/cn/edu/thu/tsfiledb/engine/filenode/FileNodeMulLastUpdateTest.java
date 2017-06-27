package cn.edu.thu.tsfiledb.engine.filenode;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.metadata.MManager;

public class FileNodeMulLastUpdateTest {

	private TSFileDBConfig tsdbconfig = TSFileDBDescriptor.getInstance().getConfig();

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
		tsconfig.rowGroupSize = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSize = 100;
		tsconfig.defaultMaxStringLength = 2;

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
	public void WriteCloseAndQuery() {

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
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
		} catch (FileNodeProcessorException e) {
			fail(e.getMessage());
			e.printStackTrace();
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
					processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, begin);
					assertEquals(true, bfProcessor.isNewProcessor());
					bfProcessor.write(deltaObjectId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
					bfProcessor.setNewProcessor(false);
					processor.addIntervalFileNode(begin, bfProcessor.getFileName());
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
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, begin);
					bfProcessor.write(deltaObjectId1, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
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
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, i);
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
