package org.apache.iotdb.db.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.db.utils.EnvironmentUtils;

public class OverflowResourceTest {

	private OverflowResource work;
	private File insertFile;
	private File updateFile;
	private File positionFile;
	private String insertFileName = "unseqTsFile";
	private String updateDeleteFileName = "overflowFile";
	private String positionFileName = "positionFile";
	private String filePath = "overflow";
	private String dataPath = "1";
	private OverflowSupport support = new OverflowSupport();

	@Before
	public void setUp() throws Exception {
		work = new OverflowResource(filePath, dataPath);
		insertFile = new File(new File(filePath, dataPath), insertFileName);
		updateFile = new File(new File(filePath, dataPath), updateDeleteFileName);
		positionFile = new File(new File(filePath, dataPath), positionFileName);
	}

	@After
	public void tearDown() throws Exception {
		work.close();
		support.clear();
		EnvironmentUtils.cleanDir(filePath);
	}

	@Test
	public void testOverflowInsert() throws IOException {
		OverflowTestUtils.produceInsertData(support);
		work.flush(OverflowTestUtils.getFileSchema(), support.getMemTabale(), null, "processorName");
		List<ChunkMetaData> chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deviceId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType2);
		assertEquals(0, chunkMetaDatas.size());
		work.appendMetadatas();
		chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
				OverflowTestUtils.dataType1);
		assertEquals(1, chunkMetaDatas.size());
		ChunkMetaData chunkMetaData = chunkMetaDatas.get(0);
		assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getTsDataType());
		assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getMeasurementUID());
		// close
		work.close();
		// append file
		long originlength = insertFile.length();
		FileOutputStream fileOutputStream = new FileOutputStream(insertFile, true);
		fileOutputStream.write(new byte[20]);
		fileOutputStream.close();
		assertEquals(originlength + 20, insertFile.length());
		work = new OverflowResource(filePath, dataPath);
		chunkMetaDatas = work.getInsertMetadatas(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
				OverflowTestUtils.dataType1);
		assertEquals(1, chunkMetaDatas.size());
		chunkMetaData = chunkMetaDatas.get(0);
		assertEquals(OverflowTestUtils.dataType1, chunkMetaData.getTsDataType());
		assertEquals(OverflowTestUtils.measurementId1, chunkMetaData.getMeasurementUID());
		assertEquals(originlength, insertFile.length());
	}
}
