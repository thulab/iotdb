package cn.edu.tsinghua.iotdb.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.auth.dao.Authorizer;
import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.auth.model.User;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class AuthorizerTest {

	private DBDao dbdao = null;

	@Before
	public void setUp() throws Exception {
		dbdao = new DBDao();
		dbdao.open();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		dbdao.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testAuthorizer() {

		IAuthorizer authorizer = Authorizer.instance;
		/**
		 * login
		 */
		boolean status = false;
		try {
			status = authorizer.login("root", "root");
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			status = authorizer.login("root", "error");
		} catch (AuthException e) {
			assertEquals("The username or the password is not correct", e.getMessage());
		}
		/**
		 * create user,delete user
		 */
		User user = new User("user", "password");
		try {
			status = authorizer.createUser(user.getUserName(), user.getPassWord());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.createUser(user.getUserName(), user.getPassWord());
		} catch (AuthException e) {
			assertEquals("The user is exist", e.getMessage());
		}
		try {
			status = authorizer.login(user.getUserName(), user.getPassWord());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.deleteUser(user.getUserName());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.deleteUser(user.getUserName());
		} catch (AuthException e) {
			assertEquals("The user is not exist", e.getMessage());
		}

		/**
		 * permission for user
		 */
		String nodeName = "root.laptop.d1";
		try {
			authorizer.createUser(user.getUserName(), user.getPassWord());
			status = authorizer.grantPrivilegeToUser(user.getUserName(), nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			authorizer.grantPrivilegeToUser(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is exist", e.getMessage());
		}
		try {
			authorizer.grantPrivilegeToUser("error", nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The user is not exist", e.getMessage());
		}
		try {
			status = authorizer.revokePrivilegeFromUser(user.getUserName(), nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.revokePrivilegeFromUser(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is not exist", e.getMessage());
		}
		try {
			authorizer.deleteUser(user.getUserName());
			authorizer.revokePrivilegeFromUser(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The user is not exist", e.getMessage());
		}
		/**
		 * role
		 */
		String roleName = "role";
		try {
			status = authorizer.createRole(roleName);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.createRole(roleName);
		} catch (AuthException e) {
			assertEquals("The role is exist", e.getMessage());
		}

		try {
			status = authorizer.deleteRole(roleName);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.deleteRole(roleName);
		} catch (AuthException e) {
			assertEquals("The role is not exist", e.getMessage());
		}
		/**
		 * role permission
		 */
		try {
			status = authorizer.createRole(roleName);
			status = authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			status = authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is exist", e.getMessage());
		}

		try {
			status = authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e1) {
			fail(e1.getMessage());
		}
		try {
			authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is not exist", e.getMessage());
		}

		try {
			authorizer.deleteRole(roleName);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			authorizer.revokePrivilegeFromRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The role is not exist", e.getMessage());
		}
		try {
			authorizer.grantPrivilegeToRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The role is not exist", e.getMessage());
		}

		/**
		 * user role
		 */
		try {
			authorizer.createUser(user.getUserName(), user.getPassWord());
			authorizer.createRole(roleName);
			status = authorizer.grantRoleToUser(roleName, user.getUserName());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			authorizer.grantPrivilegeToUser(user.getUserName(), nodeName, 1);
			authorizer.grantPrivilegeToRole(roleName, nodeName, 2);
			authorizer.grantPrivilegeToRole(roleName, nodeName, 3);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			Set<Integer> permisssions = authorizer.getPermission(user.getUserName(), nodeName);
			assertEquals(3, permisssions.size());
			assertEquals(true, permisssions.contains(1));
			assertEquals(true, permisssions.contains(2));
			assertEquals(true, permisssions.contains(3));
			assertEquals(false, permisssions.contains(4));
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.revokeRoleFromUser(roleName, user.getUserName());
			assertEquals(true, status);
			Set<Integer> permisssions = authorizer.getPermission(user.getUserName(), nodeName);
			assertEquals(1, permisssions.size());
			assertEquals(true, permisssions.contains(1));
			assertEquals(false, permisssions.contains(2));
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = authorizer.checkUserPermission(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			fail(e.getMessage());
		}
		assertEquals(true, status);
		try {
			status = authorizer.checkUserPermission(user.getUserName(), nodeName, 2);
		} catch (AuthException e) {
			fail(e.getMessage());
		}
		assertEquals(false, status);
		try {
			status = authorizer.updateUserPassword(user.getUserName(), "new");
			status = authorizer.login(user.getUserName(), "new");
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			authorizer.deleteUser(user.getUserName());
			authorizer.deleteRole(roleName);
		} catch (AuthException e) {
			e.printStackTrace();
		}
	}

}
