package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.dao.AuthDao;
import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchRoleException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchUserException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class AuthTest {

	static AuthDao dao;
	
	@BeforeClass
	public static void setup() throws AuthException {
		dao = AuthDao.getInstance();
	}
	
	@Test
	public void loginTest() throws AuthException {
		assertTrue(dao.login("root", "root"));
		boolean caught;
		caught = false;
		try {
			assertFalse(dao.login("root", "toor"));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
	}

	@Test
	public void createDeleteUserTest() throws AuthException {
		String username = "user", password = "pw";
		try {
			dao.deleteUser(username);
		} catch (Exception e) {
		}
		
		boolean caught = false;
		caught = false;
		try {
			assertFalse(dao.login(username, password));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		assertTrue(dao.addUser(username, password));
		assertTrue(dao.login(username, password));
		caught = false;
		try {
			assertFalse(dao.addUser(username, password));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		
		assertTrue(dao.deleteUser(username));
		caught = false;
		try {
			assertFalse(dao.login(username, password));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			assertFalse(dao.deleteUser(username));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		assertTrue(dao.addUser(username, password));
	}
	
	@Test
	public void createDeleteRoleTest() throws AuthException {
		String roleName = "role-create";
		try {
			dao.deleteRole(roleName);
		} catch (Exception e) {
		}
		assertTrue(dao.addRole(roleName));
		assertEquals(dao.findRole(roleName).getRoleName(), roleName);
		boolean caught = false;
		caught = false;
		try {
			assertFalse(dao.addRole(roleName));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		assertTrue(caught);
		assertTrue(dao.deleteRole(roleName));
		caught = false;
		try {
			assertFalse(dao.deleteRole(roleName));
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.findRole(roleName);
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
		assertTrue(dao.addRole(roleName));
	}
	
	@Test
	public void permissionTest() throws AuthException {
		String username = "user-permission", password = "pw";
		String path = "root.laptop.d1";
		try {
			dao.deleteUser(username);
		} catch (Exception e2) {
		}
		dao.addUser(username, password);
		// check permission
		boolean caught = false;
		assertFalse(dao.checkPermissionOnPath(username, "wrongpath", Permission.NONE));
		try {
			assertFalse(dao.checkPermissionOnPath("GHOST", path, Permission.NONE));
		} catch (Exception e) {
				caught = true;
		}
		assertTrue(caught);
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ));
		
		// grant revoke role permission
		String readerRoleName = "reader";
		try {
			dao.deleteRole(readerRoleName);
		} catch (Exception e) {
		}
		
		assertTrue(dao.addRole(readerRoleName));
		try {
			assertTrue(dao.grantRolePermission(readerRoleName, Permission.READ));
		} catch (NoSuchRoleException e) {
			fail(e.toString());
		}
		caught = false;
		try {
			dao.grantRolePermission("not a role", Permission.MODIFY);
		} catch (NoSuchRoleException e1) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.revokeRolePermission("not a role", Permission.MODIFY);
		} catch (Exception e1) {
			if(e1 instanceof NoSuchRoleException)
				caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			assertFalse(dao.revokeRolePermission(readerRoleName, Permission.MODIFY));
		} catch (Exception e1) {
			caught = true;
		}
		assertTrue(caught);
		assertTrue(dao.revokeRolePermission(readerRoleName, Permission.READ));
		assertTrue(dao.grantRolePermission(readerRoleName, Permission.READ));
		// grant role on path
		caught = false;
		try {
			dao.grantRoleOnPath("not a user", path, readerRoleName);
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.grantRoleOnPath(username, path, "not a role");
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		try {
			assertTrue(dao.grantRoleOnPath(username, path, readerRoleName));
		} catch (AuthException e) {
			fail(e.toString());
		}
		try {
			assertTrue(dao.checkPermissionOnPath(username, path, Permission.READ));
		} catch (AuthException e) {
			fail(e.toString());
		}
		caught = false;
		try {
			assertFalse(dao.grantRoleOnPath(username, path, readerRoleName));
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		// revoke role on path
		caught = false;
		try {
			dao.revokeRoleOnPath(username, "not a path", readerRoleName);
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.revokeRoleOnPath("not a user", path, readerRoleName);
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.revokeRoleOnPath(username, path, "not a role");
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		
		assertTrue(dao.revokeRoleOnPath(username, path, readerRoleName));
		caught = false;
		try {
			assertFalse(dao.revokeRoleOnPath(username, path, readerRoleName));
		} catch (AuthException e) {
			caught = true;
		}
		assertTrue(caught);
		assertTrue(dao.grantRoleOnPath(username, path, readerRoleName));

		// grant / revoke permission to role should influence user permission
		assertTrue(dao.grantRolePermission(readerRoleName, Permission.MODIFY));
		assertTrue(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.MODIFY));
		assertTrue(dao.revokeRolePermission(readerRoleName, Permission.MODIFY));
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.MODIFY));
		
		// grant / revoke role to path should influence user permission
		String writerRoleName = "writer";
		try {
			dao.addRole(writerRoleName);
			dao.grantRolePermission(writerRoleName, Permission.MODIFY);
		} catch (Exception e) {
		}	
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.MODIFY));
		dao.grantRoleOnPath(username, path, writerRoleName);
		assertTrue(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.MODIFY));
		dao.revokeRoleOnPath(username, path, readerRoleName);
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.MODIFY));
		assertTrue(dao.checkPermissionOnPath(username, path, Permission.MODIFY));
	}
}
