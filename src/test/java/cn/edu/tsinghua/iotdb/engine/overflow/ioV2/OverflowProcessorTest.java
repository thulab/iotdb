package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
		processor.clear();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testInsertUpdate() throws IOException, OverflowProcessorException, InterruptedException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		assertEquals(true, new File(PathUtils.getOverflowWriteDir(processorName), "0").exists());
		assertEquals(false, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write update data
		OverflowTestUtils.produceUpdateData(processor);
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		// flush asynchronously
		processor.flush();
		assertEquals(true, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write insert data
		OverflowTestUtils.produceInsertData(processor);
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		TimeUnit.SECONDS.sleep(1);
		assertEquals(false, processor.isFlush());
		// flush synchronously
		processor.close();
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		processor.switchWorkToMerge();
		assertEquals(true, processor.isMerge());
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		assertEquals(false, processor.canBeClosed());
		processor.queryMerge(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1,
				OverflowTestUtils.dataType1);

		processor.switchMergeToWork();
		processor.close();
	}

	@Test
	public void testWriteMemoryAndQuery() throws IOException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		OverflowTestUtils.produceUpdateData(processor);
		OverflowTestUtils.produceInsertData(processor);
		// test query
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType2);
		// no data in memory
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType2);
	}

	@Test
	public void testFlushAndQuery() throws IOException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		OverflowTestUtils.produceUpdateData(processor);
		processor.flush();
		// waiting for the end of flush.
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
		}
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		OverflowTestUtils.produceInsertData(processor);
		processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType2);
	}

	@Test
	public void testRecovery() throws OverflowProcessorException, IOException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		OverflowTestUtils.produceUpdateData(processor);
		processor.close();
		processor.switchWorkToMerge();
		assertEquals(true, processor.isMerge());
		OverflowProcessor overflowProcessor = new OverflowProcessor(processorName, parameters,
				OverflowTestUtils.getFileSchema());
		assertEquals(true, overflowProcessor.isMerge());
		// recovery query
		overflowProcessor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		processor.switchMergeToWork();
		processor.close();
		overflowProcessor.close();
		overflowProcessor.clear();
	}
}
