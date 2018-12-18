package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.footer.ChunkGroupFooter;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.write.writer.DefaultTsFileOutput;
import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;

public class BufferWriteIOTest {

	private BufferIO bufferWriteIO = null;
	private String filePath = "bufferwritepath";

	@Before
	public void setUp() throws Exception {
		DefaultTsFileOutput defaultTsFileOutput = new DefaultTsFileOutput(new FileOutputStream(new File(filePath)));
		bufferWriteIO = new BufferIO(defaultTsFileOutput, new ArrayList<>());
	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanDir(filePath);
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void test() throws IOException {
		assertEquals(TsFileIOWriter.magicStringBytes.length, bufferWriteIO.getPos());
		assertEquals(0, bufferWriteIO.getAppendedRowGroupMetadata().size());
		// construct one rowgroup
		bufferWriteIO.startFlushChunkGroup("d1");
		ChunkGroupFooter chunkGroupFooter = new ChunkGroupFooter("d1", 1000, 10);
		bufferWriteIO.endChunkGroup(chunkGroupFooter);
		assertEquals(1, bufferWriteIO.getChunkGroupMetaDatas().size());
		assertEquals(1, bufferWriteIO.getAppendedRowGroupMetadata().size());
		List<ChunkGroupMetaData> metadatas = bufferWriteIO.getAppendedRowGroupMetadata();
		ChunkGroupMetaData rowgroup = metadatas.get(0);
		assertEquals("d1", rowgroup.getDeviceID());
		// construct another two rowgroup
		bufferWriteIO.startFlushChunkGroup("d1");
		chunkGroupFooter = new ChunkGroupFooter("d1", 1000, 10);
		bufferWriteIO.endChunkGroup(chunkGroupFooter);

		bufferWriteIO.startFlushChunkGroup("d1");
		chunkGroupFooter = new ChunkGroupFooter("d1", 1000, 10);
		bufferWriteIO.endChunkGroup(chunkGroupFooter);

		bufferWriteIO.startFlushChunkGroup("d1");
		chunkGroupFooter = new ChunkGroupFooter("d1", 1000, 10);
		bufferWriteIO.endChunkGroup(chunkGroupFooter);

		metadatas = bufferWriteIO.getAppendedRowGroupMetadata();
		assertEquals(3, metadatas.size());
		FileSchema fileSchema = new FileSchema();
		bufferWriteIO.endFile(fileSchema);
	}

}
