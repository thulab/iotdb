package cn.edu.tsinghua.iotdb.qp.cud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexPlan;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;

public class IndexTest {
	

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateIndex() throws QueryProcessorException, ArgsErrorException {
		
		String createIndex = "create index on root.laptop.d1.s1 using kvindex with window_length=2, since_time=0";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan =  (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(2, indexPlan.getParameters().keySet().size());
		assertTrue(indexPlan.getParameters().containsKey("window_length"));
		assertEquals(2, indexPlan.getParameters().get("window_length"));
		assertTrue(indexPlan.getParameters().containsKey("since_time"));
		assertEquals(0L, indexPlan.getParameters().get("since_time"));
//		assertEquals(0, indexPlan.getStartTime());
	}


}
