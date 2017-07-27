package cn.edu.thu.tsfiledb.qp.cud;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.crud.IndexOperator.IndexType;
import cn.edu.thu.tsfiledb.qp.physical.crud.IndexPlan;
import cn.edu.thu.tsfiledb.qp.physical.crud.IndexQueryPlan;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;

public class IndexTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateIndex() throws QueryProcessorException, ArgsErrorException {

		String createIndex = "create index on root.laptop.d1.s1 using kv-match";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(0, indexPlan.getParameters().keySet().size());
		assertEquals(0, indexPlan.getStartTime());
	}

	@Test
	public void testCreateIndex2() throws QueryProcessorException, ArgsErrorException {
		String createIndex = "create index on root.laptop.d1.s1 using kv-match with b=20,a=50 where time>=100";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals("root.laptop.d1.s1", indexPlan.getPaths().get(0).getFullPath());
		assertEquals(2, indexPlan.getParameters().keySet().size());
		Map<String, Object> map = indexPlan.getParameters();
		assertEquals((long) 20, (long) map.get("b"));
		assertEquals((long) 50, (long) map.get("a"));
		assertEquals(100, indexPlan.getStartTime());
		assertEquals(IndexType.CREATE_INDEX, indexPlan.getIndexType());
		createIndex = "create index on root.laptop.d1.s1 using kv-match with b=20,a=50 where time>100";
		processor = new QueryProcessor(new MemIntQpExecutor());
		indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(createIndex);
		assertEquals(101, indexPlan.getStartTime());
		assertEquals(IndexType.CREATE_INDEX, indexPlan.getIndexType());
	}

	@Test
	public void testDropIndex() throws QueryProcessorException, ArgsErrorException {
		String sql = "drop index on root.laptop.d1.s1";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexPlan indexPlan = (IndexPlan) processor.parseSQLToPhysicalPlan(sql);
		assertEquals(indexPlan.getPaths().get(0).getFullPath(), "root.laptop.d1.s1");
		assertEquals(IndexType.DROP_INDEX, indexPlan.getIndexType());
	}

	@Test
	public void testSelectIndex() throws QueryProcessorException, ArgsErrorException {
		String sql = "select index subsequence_matching(root.laptop.d1.s1,root.a.b.c,123,132,123.1)";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexQueryPlan indexQueryPlan = (IndexQueryPlan) processor.parseSQLToPhysicalPlan(sql);
		assertEquals("root.laptop.d1.s1", indexQueryPlan.getPaths().get(0).getFullPath());
		assertEquals(123.1, indexQueryPlan.getEpsilon(), 1E-2);
		assertEquals(0, indexQueryPlan.getStartTime());
		assertEquals(Long.MAX_VALUE, indexQueryPlan.getEndTime());
		assertEquals("root.a.b.c", indexQueryPlan.getPatterPath().getFullPath());
		assertEquals(123, indexQueryPlan.getPatterStarTime());
		assertEquals(132, indexQueryPlan.getPatterEndTime());
	}

	@Test
	public void testSelectIndex2() throws QueryProcessorException, ArgsErrorException {
		String sql = "select index subsequence_matching(root.laptop.d1.s1,root.a.b.c,123,132,123.1,11.1,22.2) where time>100 and time<200 ";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexQueryPlan indexQueryPlan = (IndexQueryPlan) processor.parseSQLToPhysicalPlan(sql);
		assertEquals("root.laptop.d1.s1", indexQueryPlan.getPaths().get(0).getFullPath());
		assertEquals(123.1, indexQueryPlan.getEpsilon(), 1E-2);
		assertEquals(101, indexQueryPlan.getStartTime());
		assertEquals(199, indexQueryPlan.getEndTime());
		assertEquals("root.a.b.c", indexQueryPlan.getPatterPath().getFullPath());
		assertEquals(11.1, indexQueryPlan.getAlpha(), 1E-2);
		assertEquals(22.2, indexQueryPlan.getBeta(), 1E-2);
		assertEquals(123, indexQueryPlan.getPatterStarTime());
		assertEquals(132, indexQueryPlan.getPatterEndTime());
	}

	@Test
	public void testSelectIndex3() {
		String sql = "select index subsequence_matching(root.laptop.d1.s1, root.a.b.c,123,132,123.1,11.1,22.2) where time>200 and time<100 ";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexQueryPlan indexQueryPlan;
		try {
			indexQueryPlan = (IndexQueryPlan) processor.parseSQLToPhysicalPlan(sql);
			assertEquals("root.laptop.d1.s1", indexQueryPlan.getPaths().get(0).getFullPath());
			assertEquals("root.a.b.c", indexQueryPlan.getPatterPath().getFullPath());
			assertEquals(11.1, indexQueryPlan.getAlpha(), 1E-2);
			assertEquals(22.2, indexQueryPlan.getBeta(), 1E-2);
			assertEquals(123, indexQueryPlan.getPatterStarTime());
			assertEquals(132, indexQueryPlan.getPatterEndTime());
		} catch (QueryProcessorException | ArgsErrorException e) {
			assertEquals("for index query command, the time filter must be an interval", e.getMessage());
		}
		sql = "select index subsequence_matching(root.laptop.d1.s1, root.a.b.c,123,132,123.1,11.1,22.2) where time<-1 and time>-100";
		try {
			indexQueryPlan = (IndexQueryPlan) processor.parseSQLToPhysicalPlan(sql);
			assertEquals("root.laptop.d1.s1", indexQueryPlan.getPaths().get(0).getFullPath());
		} catch (QueryProcessorException | ArgsErrorException e) {
			assertEquals("index query time must be greater than 0.", e.getMessage());
		}

		sql = "select index subsequence_matching(root.laptop.d1.s1, root.a.b.c,123,132,123.1,11.1,22.2) where time<100";
		try {
			indexQueryPlan = (IndexQueryPlan) processor.parseSQLToPhysicalPlan(sql);
			assertEquals("root.laptop.d1.s1", indexQueryPlan.getPaths().get(0).getFullPath());
			assertEquals(99, indexQueryPlan.getEndTime());
			assertEquals(0, indexQueryPlan.getStartTime());
			assertEquals("root.a.b.c", indexQueryPlan.getPatterPath().getFullPath());
			assertEquals(11.1, indexQueryPlan.getAlpha(), 1E-2);
			assertEquals(22.2, indexQueryPlan.getBeta(), 1E-2);
			assertEquals(123, indexQueryPlan.getPatterStarTime());
			assertEquals(132, indexQueryPlan.getPatterEndTime());
		} catch (QueryProcessorException | ArgsErrorException e) {
			fail(e.getMessage());
		}
	}
	@Test
	public void testSelectIndex4(){
		String sql = "select index subsequence_matching(root.laptop.d1.s1, root.a.b.c,2016-11-16T16:22:33+08:00, now(),123.1,11.1,22.2) where time<2016-11-16T16:22:33+08:00";
		QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
		IndexQueryPlan indexQueryPlan;
		try {
			indexQueryPlan = (IndexQueryPlan) processor.parseSQLToPhysicalPlan(sql);
			assertEquals("root.laptop.d1.s1", indexQueryPlan.getPaths().get(0).getFullPath());
			assertEquals("root.a.b.c", indexQueryPlan.getPatterPath().getFullPath());
			assertEquals(11.1, indexQueryPlan.getAlpha(), 1E-2);
			assertEquals(22.2, indexQueryPlan.getBeta(), 1E-2);
		} catch (QueryProcessorException | ArgsErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
