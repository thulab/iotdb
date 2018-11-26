package cn.edu.tsinghua.iotdb.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import cn.edu.tsinghua.iotdb.client.AbstractClient.OPERATION_RESULT;
import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;
import cn.edu.tsinghua.iotdb.jdbc.TsfileDatabaseMetadata;

public class AbstractClientTest {
    @Mock
    private TsfileConnection connection;
    
    @Mock
    private TsfileDatabaseMetadata databaseMetadata;
    
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(connection.getMetaData()).thenReturn(databaseMetadata);
		when(databaseMetadata.getMetadataInJson()).thenReturn("test metadata");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testInit() {
		AbstractClient.init();
		String[] keywords = {
				AbstractClient.HOST_ARGS, 
				AbstractClient.HELP_ARGS,
				AbstractClient.PORT_ARGS,
				AbstractClient.PASSWORD_ARGS,
				AbstractClient.USERNAME_ARGS,
				AbstractClient.ISO8601_ARGS,
				AbstractClient.MAX_PRINT_ROW_COUNT_ARGS, 
		};
		for(String keyword: keywords) {
			if(!AbstractClient.keywordSet.contains("-"+keyword)) {
				System.out.println(keyword);
				fail();
			}
		}
	}

//	@Test
//	public void testOutput() {
//		fail("Not yet implemented");
//	}
//
//
//	@Test
//	public void testCheckRequiredArg() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testSetMaxDisplayNumber() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testPrintBlockLine() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testPrintName() {
//		fail("Not yet implemented");
//	}
//
	@Test
	public void testCheckPasswordArgs() {
		AbstractClient.init();
		String[] input = new String[] {"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
		String[] res = new String[] {"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
		isTwoStringArrayEqual(res, AbstractClient.checkPasswordArgs(input));
		
		input = new String[]{"-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root"};
		res = new String[]{"-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root"};
		isTwoStringArrayEqual(res, AbstractClient.checkPasswordArgs(input));

		input = new String[]{"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root", "-pw"};
		res = new String[]{"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
		isTwoStringArrayEqual(res, AbstractClient.checkPasswordArgs(input));

		input = new String[]{"-h", "127.0.0.1", "-p", "6667", "-pw", "-u", "root"};
		res = new String[]{"-h", "127.0.0.1", "-p", "6667", "-u", "root"};
		isTwoStringArrayEqual(res, AbstractClient.checkPasswordArgs(input));

		input = new String[]{"-pw", "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
		res = new String[]{"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
		isTwoStringArrayEqual(res, AbstractClient.checkPasswordArgs(input));

		input = new String[]{};
		res = new String[]{};
		isTwoStringArrayEqual(res, AbstractClient.checkPasswordArgs(input));
	}
	
	private void isTwoStringArrayEqual(String[] expected, String[] actual) {
		for(int i = 0; i < expected.length;i++) {
			assertEquals(expected[i], actual[i]);
		}
	}

	@Test
	public void testHandleInputInputCmd() {
		assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.EXIT_COMMAND, connection), OPERATION_RESULT.RETURN_OPER);
		assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.QUIT_COMMAND, connection), OPERATION_RESULT.RETURN_OPER);
		
		assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.SHOW_METADATA_COMMAND, connection), OPERATION_RESULT.CONTINUE_OPER);
		
		assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=", AbstractClient.SET_TIMESTAMP_DISPLAY), connection), OPERATION_RESULT.CONTINUE_OPER);
		assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=xxx", AbstractClient.SET_TIMESTAMP_DISPLAY), connection), OPERATION_RESULT.CONTINUE_OPER);

		
		
		
		
		
		
		
		
		
		
	}

}
