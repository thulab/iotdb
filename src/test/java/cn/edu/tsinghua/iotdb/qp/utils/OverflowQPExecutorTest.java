package cn.edu.tsinghua.iotdb.qp.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.qp.executor.OverflowQPExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.sys.MetadataOperator;
import cn.edu.tsinghua.iotdb.qp.physical.sys.MetadataPlan;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

public class OverflowQPExecutorTest {

	private OverflowQPExecutor executor = new OverflowQPExecutor();

	private String[] paths = { "root.laptop.d1.s1", "root.laptop.d1.s2", "root.laptop.d1.s3" };
	private String[] dataTypes = { "INT32", "INT64", "FLOAT" };
	private String[] encodings = { "RLE", "RLE", "RLE" };
	private List<Path> deletePath = new ArrayList<>();
	private String[] encodingArgs = new String[0];
	private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();
	private MManager manager = MManager.getInstance();

	@Before
	public void setUp() throws Exception {
		manager.flushObjectToFile();
		File file = new File(tsdbconfig.dataDir);
		if (file.exists()) {
			FileUtils.deleteDirectory(new File(tsdbconfig.dataDir));
		}
	}

	@After
	public void tearDown() throws Exception {
		manager.flushObjectToFile();
		FileUtils.deleteDirectory(new File(tsdbconfig.dataDir));
	}

	@Test
	public void MetadataDeleteTest() throws ProcessorException, PathErrorException {

		// add path
		MetadataPlan plan = new MetadataPlan(MetadataOperator.NamespaceType.ADD_PATH, new Path(paths[0]), dataTypes[0],
				encodings[0], encodingArgs, deletePath);
		executor.processNonQuery(plan);
		plan.setPath(new Path(paths[1]));
		executor.processNonQuery(plan);
		plan.setPath(new Path(paths[2]));
		executor.processNonQuery(plan);
		List<String> paths = manager.getPaths("root.laptop.d1");
		assertEquals(paths.size(), this.paths.length);
		for (int i = 0, len = paths.size(); i < len; i++) {
			assertEquals(paths.get(i), this.paths[i]);
		}
		// delete one path
		deletePath.clear();
		deletePath.add(new Path(this.paths[0]));
		plan = new MetadataPlan(MetadataOperator.NamespaceType.DELETE_PATH, null, null, null, encodingArgs, deletePath);
		executor.processNonQuery(plan);
		paths = manager.getPaths("root.laptop.d1");
		assertEquals(paths.size(), 2);
		// delete timeseries which is not exist
		deletePath.clear();
		deletePath.add(new Path(this.paths[1]));
		deletePath.add(new Path("root.laptop.d2"));
		plan = new MetadataPlan(MetadataOperator.NamespaceType.DELETE_PATH, null, null, null, encodingArgs, deletePath);
		try {
			executor.processNonQuery(plan);
		} catch (Exception e) {
			assertEquals(String.format("The timeseries %s does not exist and can't be deleted", "root.laptop.d2"),
					e.getMessage());
		}
		paths = manager.getPaths("root.laptop");
		assertEquals(paths.size(), 2);
	}
}
