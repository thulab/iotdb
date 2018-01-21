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
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

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
		OverflowSeriesDataSource overflowSeriesDataSource = processor.query(OverflowTestUtils.deltaObjectId1,
				OverflowTestUtils.measurementId1, null, null, null, OverflowTestUtils.dataType1);
		assertEquals(OverflowTestUtils.dataType1, overflowSeriesDataSource.getDataType());
		assertEquals(true, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
		assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
		assertEquals(0,
				overflowSeriesDataSource.getOverflowInsertFileList().get(0).getTimeSeriesChunkMetaDatas().size());
		assertEquals(1, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().size());
		assertEquals(0, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().get(0)
				.getTimeSeriesChunkMetaDataList().size());
		assertEquals(OverflowTestUtils.dataType1,
				overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getDataType());
		DynamicOneColumnData updateMem = overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries()
				.getOverflowUpdateInMem();
		// time :[2,10] [20,30] value: int [10,10] int[20,20]
		assertEquals(2, updateMem.getTime(0));
		assertEquals(10, updateMem.getTime(1));
		assertEquals(20, updateMem.getTime(2));
		assertEquals(30, updateMem.getTime(3));

		assertEquals(10, updateMem.getInt(0));
		assertEquals(20, updateMem.getInt(1));
		// flush asynchronously
		processor.flush();
		assertEquals(true, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write insert data
		OverflowTestUtils.produceInsertData(processor);
		overflowSeriesDataSource = processor.query(OverflowTestUtils.deltaObjectId1, OverflowTestUtils.measurementId1, null, null, null,
				OverflowTestUtils.dataType1);
		TimeUnit.SECONDS.sleep(1);
		assertEquals(false, processor.isFlush());
		assertEquals(OverflowTestUtils.dataType1, overflowSeriesDataSource.getDataType());
		assertEquals(false, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
		assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
		assertEquals(null, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateInMem());
		assertEquals(1, overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateFileList().get(0).getTimeSeriesChunkMetaDataList().size());
		for(int i = 1;i<=3;i++){
			overflowSeriesDataSource.getRawSeriesChunk().getIterator().hasNext();
			TimeValuePair pair = overflowSeriesDataSource.getRawSeriesChunk().getIterator().next();
			assertEquals(i, pair.getTimestamp());
			assertEquals(i, pair.getValue().getInt());
		}
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
