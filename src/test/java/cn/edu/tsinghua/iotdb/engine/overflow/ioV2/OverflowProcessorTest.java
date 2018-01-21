package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class OverflowProcessorTest {

	private String processorName = "test";
	private OverflowProcessor processor = null;
	private Map<String, Object> parameters = null;

	private Action overflowflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("overflow flush action");
		}
	};

	private Action filenodeflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode flush action");
		}
	};

	@Before
	public void setUp() throws Exception {
		EnvironmentUtils.envSetUp();
		parameters = new HashMap<String, Object>();
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);

	}

	@After
	public void tearDown() throws Exception {
		processor.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void test() throws IOException, OverflowProcessorException {
		processor = new OverflowProcessor(processorName, parameters);
		assertEquals(true, new File(PathUtils.getOverflowWriteDir(processorName), "0").exists());
		assertEquals(false, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write update data
		OverflowTestUtils.produceUpdateData(processor);
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		processor.flush();
		assertEquals(true, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write insert data
		OverflowTestUtils.produceInsertData(processor);
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		assertEquals(false, processor.isFlush());
		processor.close();
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		processor.switchWorkToMerge();
		assertEquals(true, processor.isMerge());
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		assertEquals(false, processor.canBeClosed());
		processor.switchMergeToWork();
		processor.close();
	}
	//
	// @Test
	// public void testWriteMemoryAndQuery() {
	//
	// }
	//
	// @Test
	// public void testFlushAndQuery() {
	//
	// }
	//
	// @Test
	// public void testRecovery() {
	//
	// }

}
