package cn.edu.tsinghua.iotdb.qp.query;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.logical.crud.IndexQueryOperator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.IndexQueryPlan;
import cn.edu.tsinghua.iotdb.qp.physical.index.KvMatchIndexQueryPlan;
import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KvIndexQueryTest {
	private static double minDoubleDelta = Double.MIN_VALUE;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testQueryIndexSingleSideWhere() throws QueryProcessorException, ArgsErrorException {
		
		String queryIndex = "select kvindex(root.c, 1, 2, 0.5, 3.0, 4.0) from root.a.b where time >= 5";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		PhysicalPlan plan =  processor.parseSQLToPhysicalPlan(queryIndex);
		assertTrue(plan instanceof KvMatchIndexQueryPlan);
		KvMatchIndexQueryPlan indexQueryPlan = (KvMatchIndexQueryPlan) plan;
		assertEquals("root.c", indexQueryPlan.getPatterPath().toString());
		assertEquals("[root.a.b]", indexQueryPlan.getPaths().toString());
		assertEquals(1, indexQueryPlan.getPatternStartTime());
		assertEquals(2, indexQueryPlan.getPatternEndTime());
		assertEquals(5, indexQueryPlan.getStartTime());
		assertEquals(Long.MAX_VALUE, indexQueryPlan.getEndTime());
		assertEquals(0.5d, indexQueryPlan.getEpsilon(), minDoubleDelta);
		assertEquals(3d, indexQueryPlan.getAlpha(), minDoubleDelta);
		assertEquals(4d, indexQueryPlan.getBeta(), minDoubleDelta);
	}

	@Test
	public void testQueryIndexBothSideWhere() throws QueryProcessorException, ArgsErrorException {
		String queryIndex = "select kvindex(root.c, 1, 2, 0.5, 3.0, 4.0) from root.a.b where time >= 5 and time <= 10";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		PhysicalPlan plan =  processor.parseSQLToPhysicalPlan(queryIndex);
		assertTrue(plan instanceof KvMatchIndexQueryPlan);
		KvMatchIndexQueryPlan indexQueryPlan = (KvMatchIndexQueryPlan) plan;
		assertEquals("root.c", indexQueryPlan.getPatterPath().toString());
		assertEquals("[root.a.b]", indexQueryPlan.getPaths().toString());
		assertEquals(1, indexQueryPlan.getPatternStartTime());
		assertEquals(2, indexQueryPlan.getPatternEndTime());
		assertEquals(5, indexQueryPlan.getStartTime());
		assertEquals(10, indexQueryPlan.getEndTime());
		assertEquals(0.5d, indexQueryPlan.getEpsilon(), minDoubleDelta);
		assertEquals(3d, indexQueryPlan.getAlpha(), minDoubleDelta);
		assertEquals(4d, indexQueryPlan.getBeta(), minDoubleDelta);

	}

}
