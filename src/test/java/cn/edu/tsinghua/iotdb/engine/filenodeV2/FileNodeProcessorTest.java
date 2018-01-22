package cn.edu.tsinghua.iotdb.engine.filenodeV2;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UpdateDeleteInfoOfOneSeries;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class FileNodeProcessorTest {

	private FileNodeProcessor fileNodeProcessor;
	private String processorName = "root.vehicle.d0";
	private String measurementId = "s0";
	private TSDataType dataType = TSDataType.INT32;
	private Map<String, Object> parameters = null;
	private int groupThreshold;
	private TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		groupThreshold = config.groupSizeInByte;
		config.groupSizeInByte = 1024;
		parameters = new HashMap<>();
		MetadataManagerHelper.initMetadata();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		config.groupSizeInByte = groupThreshold;
		fileNodeProcessor.delete();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void test() throws Exception {
		fileNodeProcessor = new FileNodeProcessor("data", processorName, parameters);
		assertEquals(false, fileNodeProcessor.shouldRecovery());
		assertEquals(false, fileNodeProcessor.isOverflowed());
		assertEquals(-1, fileNodeProcessor.getLastUpdateTime(processorName));
		for (int i = 1; i <= 100; i++) {
			long flushLastUpdateTime = fileNodeProcessor.getFlushLastUpdateTime(processorName);
			if (i < flushLastUpdateTime) {
				// overflow
				fail("overflow data");
			} else {
				// bufferwrite
				BufferWriteProcessor bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(processorName,
						i + System.currentTimeMillis());
				if (bufferWriteProcessor.isNewProcessor()) {
					bufferWriteProcessor.setNewProcessor(false);
					String bufferwriteRelativePath = bufferWriteProcessor.getFileRelativePath();
					fileNodeProcessor.addIntervalFileNode(i, bufferwriteRelativePath);
				}
				bufferWriteProcessor.write(processorName, measurementId, i, dataType, String.valueOf(i));
				fileNodeProcessor.setIntervalFileNodeStartTime(processorName, i);
				fileNodeProcessor.setLastUpdateTime(processorName, i);
			}
			if (i == 85) {

			} else if (i == 87) {
				// the groupSize is 1024Bytes. The size of one INT32 data-point is 12Bytes
				// the flush will be triggered when the number of insert data reaches 86(1024/12=85.33)
				TimeUnit.SECONDS.sleep(2);
				// query result contains the flushed result.
				fileNodeProcessor.getOverflowProcessor(processorName, parameters);
				QueryDataSource dataSource = fileNodeProcessor.query(processorName, measurementId, null, null, null);
				// overflow data
				OverflowSeriesDataSource overflowSeriesDataSource = dataSource.getOverflowSeriesDataSource();
				assertEquals(processorName + "." + measurementId,
						overflowSeriesDataSource.getSeriesPath().getFullPath());
				assertEquals(dataType, overflowSeriesDataSource.getDataType());
				assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
				assertEquals(0, overflowSeriesDataSource.getOverflowInsertFileList().get(0)
						.getTimeSeriesChunkMetaDatas().size());
				assertEquals(true, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
				UpdateDeleteInfoOfOneSeries deleteInfoOfOneSeries = overflowSeriesDataSource
						.getUpdateDeleteInfoOfOneSeries();
				assertEquals(dataType, deleteInfoOfOneSeries.getDataType());
				assertEquals(null, deleteInfoOfOneSeries.getOverflowUpdateInMem());
				assertEquals(1, deleteInfoOfOneSeries.getOverflowUpdateFileList().size());
				assertEquals(0, deleteInfoOfOneSeries.getOverflowUpdateFileList().get(0)
						.getTimeSeriesChunkMetaDataList().size());
				// bufferwrite data | sorted tsfile data
				GlobalSortedSeriesDataSource globalSortedSeriesDataSource = dataSource.getSeriesDataSource();
				assertEquals(processorName + "." + measurementId,
						globalSortedSeriesDataSource.getSeriesPath().toString());
				assertEquals(0, globalSortedSeriesDataSource.getSealedTsFiles().size());
				assertEquals(processorName + "." + measurementId,
						globalSortedSeriesDataSource.getUnsealedTsFile().getFilePath());
				assertEquals(1, globalSortedSeriesDataSource.getUnsealedTsFile().getTimeSeriesChunkMetaDatas().size());
				assertEquals(false, globalSortedSeriesDataSource.getRawSeriesChunk().isEmpty());
				assertEquals(87, globalSortedSeriesDataSource.getRawSeriesChunk().getMaxTimestamp());
				assertEquals(87, globalSortedSeriesDataSource.getRawSeriesChunk().getMinTimestamp());
				assertEquals(87, globalSortedSeriesDataSource.getRawSeriesChunk().getMaxValue().getInt());
			}
		}
		assertEquals(true, fileNodeProcessor.canBeClosed());
		fileNodeProcessor.close();
	}

}
