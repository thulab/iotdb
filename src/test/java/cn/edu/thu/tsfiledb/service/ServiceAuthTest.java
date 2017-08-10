package cn.edu.thu.tsfiledb.service;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import cn.edu.thu.tsfiledb.service.rpc.thrift.TSCloseOperationReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteStatementReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSExecuteStatementResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSOpenSessionReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TS_StatusCode;

public class ServiceAuthTest {
	
	static TSServiceImpl tsServiceImpl;
	static TSOpenSessionReq req;
	
	@BeforeClass
	public static void setup() throws IOException {
		tsServiceImpl = new TSServiceImpl();
		req = new TSOpenSessionReq();
	}
	
	@Test
	public void rootTest() throws IOException, TException {
		
		String username = "root", password = "root";
		req.setUsername(username);
		req.setPassword(password);
		tsServiceImpl.openSession(req);
		
		TSExecuteStatementReq exeReq = new TSExecuteStatementReq();
		TSExecuteStatementResp exeResp;
	
		exeReq.setStatement("drop role testrole");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		exeReq.setStatement("create role testrole");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(), TS_StatusCode.SUCCESS_STATUS, exeResp.getStatus().getStatusCode());
		
		exeReq.setStatement("grant role testrole privileges 'read','modify'");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(), TS_StatusCode.SUCCESS_STATUS, exeResp.getStatus().getStatusCode());
		
		exeReq.setStatement("grant to user root role testrole on root.a.b.c");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(), TS_StatusCode.SUCCESS_STATUS, exeResp.getStatus().getStatusCode());
		
		exeReq.setStatement("show privileges on root.a.b.c");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		assertEquals("Privileges are READ,WRITE",exeResp.getStatus().getErrorMessage());
	}

	@Test
	public void nonRootTest() throws TException {
		String rootname = "root", rootPW = "root";
		req.setUsername(rootname);
		req.setPassword(rootPW);
		tsServiceImpl.openSession(req);
		
		TSExecuteStatementReq exeReq = new TSExecuteStatementReq();
		TSExecuteStatementResp exeResp;
		// root create a user
		exeReq.setStatement("drop user nonroot");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		exeReq.setStatement("create user nonroot 123456");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		// switch to user
		tsServiceImpl.closeOperation(new TSCloseOperationReq());
		String username = "nonroot", password = "123456";
		req.setUsername(username);
		req.setPassword(password);
		assertEquals(TS_StatusCode.SUCCESS_STATUS,tsServiceImpl.openSession(req).getStatus().getStatusCode());
		// user currently have no permission
		exeReq.setStatement("show privileges on root.a.b.c");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.ERROR_STATUS,exeResp.getStatus().getStatusCode());
		// user cannot create a role
		exeReq.setStatement("create role userrole");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.ERROR_STATUS,exeResp.getStatus().getStatusCode());
		// switch to root
		tsServiceImpl.closeOperation(new TSCloseOperationReq());
		req.setUsername(rootname);
		req.setPassword(rootPW);
		tsServiceImpl.openSession(req);
		// root creates role and grants to user
		exeReq.setStatement("drop role userrole");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		exeReq.setStatement("create role userrole");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		exeReq.setStatement("grant role userrole privileges 'read'");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		exeReq.setStatement("grant to user nonroot role userrole on root.a.b.c");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		// switch to user
		tsServiceImpl.closeOperation(new TSCloseOperationReq());
		req.setUsername(username);
		req.setPassword(password);
		assertEquals(TS_StatusCode.SUCCESS_STATUS,tsServiceImpl.openSession(req).getStatus().getStatusCode());
		// user now has permission
		exeReq.setStatement("show privileges on root.a.b");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		assertEquals("Privileges are NONE",exeResp.getStatus().getErrorMessage());
		exeReq.setStatement("show privileges on root.a.b.c");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		assertEquals("Privileges are READ",exeResp.getStatus().getErrorMessage());
		exeReq.setStatement("show role on root.a.b.c");
		exeResp = tsServiceImpl.executeStatement(exeReq);
		assertEquals(exeResp.getStatus().toString(),TS_StatusCode.SUCCESS_STATUS,exeResp.getStatus().getStatusCode());
		assertEquals("Roles are userrole",exeResp.getStatus().getErrorMessage());
	}
}
