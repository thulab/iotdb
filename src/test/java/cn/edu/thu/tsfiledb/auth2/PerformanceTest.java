package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth.AuthException;
import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.dao.DBDao;
import cn.edu.thu.tsfiledb.auth2.dao.AuthDao;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class PerformanceTest {
	
	private static DBDao dbdao = null;
	private static TsfileDBConfig dbconfig = TsfileDBDescriptor.getInstance().getConfig();

	@BeforeClass
	public static void setUp() throws Exception {
		dbconfig.derbyHome = "";
		dbdao = new DBDao();
		dbdao.open();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		dbdao.close();
	}

	@Test
	public void testCreatOld() throws AuthException {
		// create 10000 users and test logging in for them
		for(int i = 0; i < 10000; i++) {
			try {
				Authorizer.deleteUser("user" + i);
			} catch (Exception e) {
			}
			Authorizer.createUser("user" + i, "password" + i);
		}
		for(int i = 0; i < 10000; i++) {
			assertTrue(Authorizer.login("user" + i, "password" + i));
		}
	}
	
	@Test
	public void testCreateNew() throws cn.edu.thu.tsfiledb.auth2.exception.AuthException {
		// create 10000 users and test logging in for them
		for(int i = 0; i < 10000; i++) {
			try {
				AuthDao.getInstance().deleteUser("user" + i);
			} catch (Exception e) {
			}	
			AuthDao.getInstance().addUser("user" + i, "password" + i);
		}
		for(int i = 0; i < 10000; i++) {
			assertTrue(AuthDao.getInstance().login("user" + i, "password" + i));
		}	
	}
	
	@Test
	public void testGrantOld() throws AuthException {
		// grant permissions to 10000 nodes and test these permissions
		String username = "grantTester", password = "123456";
		String nodeName = "root.a.b.c.d.e";
		try {
			Authorizer.createUser(username, password);
		} catch (Exception e) {
		}
		
		for(int i = 1; i < 10000; i++) {
			try {
				Authorizer.removePmsFromUser(username, nodeName + "." + i, i);
			} catch (Exception e) {
			}
			assertTrue(Authorizer.addPmsToUser(username, nodeName + "." + i, i));
		}
		
		for(int i = 1; i < 10000; i++) {
			assertTrue(String.valueOf(i), Authorizer.checkUserPermission(username, nodeName + "." + i, i));
		}
	}
	
	@Test
	public void testGrantNew() throws cn.edu.thu.tsfiledb.auth2.exception.AuthException {
		// grant permissions to 10000 nodes and test these permissions
		String username = "grantTester", password = "123456";
		String roleName = "grantRole", nodeName = "root.a.b.c.d.e";
		try {
			AuthDao.getInstance().addUser(username, password);
		} catch (Exception e) {
		}
		try {
			AuthDao.getInstance().addRole(roleName);
		} catch (Exception e) {
		}
		for(int i = 1; i < 10000; i++) {
			try {
				AuthDao.getInstance().revokeRolePermission(roleName, i);
			} catch (Exception e) {
			}
			assertTrue(AuthDao.getInstance().grantRolePermission(roleName, i));
			try {
				AuthDao.getInstance().revokeRoleOnPath(username, nodeName + "." + i, roleName);
			} catch (Exception e) {
			}
			assertTrue(AuthDao.getInstance().grantRoleOnPath(username, nodeName + "." + i, roleName));
		}
		for(int i = 1; i < 10000; i++) {
			assertTrue(AuthDao.getInstance().checkPermissionOnPath(username, nodeName + "." + i, i));
		}
	}
}
