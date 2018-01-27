package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;

import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.bufferwriteV2.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.metadata.ColumnSchema;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class BufferWriteProcessorTest {
	
	Action bfflushaction = new Action() {

		@Override
		public void act() throws Exception {

		}
	};

	Action bfcloseaction = new Action() {

		@Override
		public void act() throws Exception {
		}
	};

	Action fnflushaction = new Action() {

		@Override
		public void act() throws Exception {

		}
	};


	private int groupSizeInByte;
	private BufferWriteProcessor bufferWriteProcessor;
	private TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		// origin value
		groupSizeInByte = TsFileConf.groupSizeInByte;
		// new value
		TsFileConf.groupSizeInByte = 2000;
		// init metadata
		MetadataManagerHelper.initMetadata();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		// recovery value
		TsFileConf.groupSizeInByte = groupSizeInByte;
		// clean environment
		EnvironmentUtils.cleanEnv();
	}


}
