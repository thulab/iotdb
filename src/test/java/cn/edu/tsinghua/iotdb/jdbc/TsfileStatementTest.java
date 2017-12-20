package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_SessionHandle;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSIService.Iface;



public class TsfileStatementTest {
	@Mock
	private TsfileConnection connection;
	
	@Mock
	private Iface client;
	
	@Mock
	private TS_SessionHandle sessHandle;
	

	
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
	}


	@After
	public void tearDown() throws Exception {
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testSetFetchSize1() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    stmt.setFetchSize(123);
	    assertEquals(123, stmt.getFetchSize());
	}

	@SuppressWarnings("resource")
	@Test
	public void testSetFetchSize2() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    int initial = stmt.getFetchSize();
	    stmt.setFetchSize(0);
	    assertEquals(initial, stmt.getFetchSize());
	}
	
	@SuppressWarnings("resource")
	@Test(expected = SQLException.class)
	public void testSetFetchSize3() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    stmt.setFetchSize(-1);
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testSetMaxRows1() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
	    stmt.setMaxRows(123);
	    assertEquals(123, stmt.getMaxRows());
	}
	
	@SuppressWarnings("resource")
	@Test(expected = SQLException.class)
	public void testSetMaxRows2() throws SQLException {
		TsfileStatement stmt = new TsfileStatement(connection, client, sessHandle);
		stmt.setMaxRows(-1);
	}
}
