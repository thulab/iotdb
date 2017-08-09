package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.dao.AuthDao;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchPermException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchRoleException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchUserException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class AuthTest {

	static AuthDao dao;
	
	@BeforeClass
	public static void setup() throws IOException {
		dao = AuthDao.getInstance();
	}
	
	@Test
	public void loginTest() {
		assertTrue(dao.login("root", "root"));
		assertFalse(dao.login("root", "toor"));
	}

	@Test
	public void createDeleteUserTest() {
		String username = "user", password = "pw";
		dao.deleteUser(username);
		assertFalse(dao.login(username, password));
		assertTrue(dao.addUser(username, password));
		assertTrue(dao.login(username, password));
		assertFalse(dao.addUser(username, password));
		
		assertTrue(dao.deleteUser(username));
		assertFalse(dao.login(username, password));
		assertFalse(dao.deleteUser(username));
		assertTrue(dao.addUser(username, password));
	}
	
	@Test
	public void createDeleteRoleTest() throws NoSuchRoleException {
		String roleName = "role-create";
		dao.deleteRole(roleName);
		assertTrue(dao.addRole(roleName));
		assertEquals(dao.findRole(roleName).getRoleName(), roleName);
		assertFalse(dao.addRole(roleName));
		
		assertTrue(dao.deleteRole(roleName));
		assertFalse(dao.deleteRole(roleName));
		try {
			dao.findRole(roleName);
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchRoleException);
		}
		assertTrue(dao.addRole(roleName));
	}
	
	@Test
	public void permissionTest() throws NoSuchRoleException, PathErrorException, NoSuchUserException, NoSuchPermException {
		String username = "user-permission", password = "pw";
		String path = "root.laptop.d1";
		dao.deleteUser(username);
		dao.addUser(username, password);
		// check permission
		boolean caught = false;
		try {
			dao.checkPermissionOnPath(username, "wrongpath", Permission.NONE);
		} catch (Exception e) {
			if(e instanceof PathErrorException)
				caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.checkPermissionOnPath("GHOST", path, Permission.NONE);
		} catch (Exception e) {
			if(e instanceof  NoSuchUserException)
				caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ));
		} catch (NoSuchUserException | PathErrorException e) {
			if(e instanceof PathErrorException)
				caught = true;
		}
		assertTrue(caught);
		// grant revoke role permission
		String readerRoleName = "reader";
		dao.deleteRole(readerRoleName);
		assertTrue(dao.addRole(readerRoleName));
		try {
			assertTrue(dao.grantRolePermission(readerRoleName, Permission.READ));
		} catch (NoSuchRoleException e) {
			fail(e.toString());
		}
		caught = false;
		try {
			dao.grantRolePermission("not a role", Permission.WRITE);
		} catch (NoSuchRoleException e1) {
			caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.revokeRolePermission("not a role", Permission.WRITE);
		} catch (Exception e1) {
			if(e1 instanceof NoSuchRoleException)
				caught = true;
		}
		assertTrue(caught);
		assertFalse(dao.revokeRolePermission(readerRoleName, Permission.WRITE));
		assertTrue(dao.revokeRolePermission(readerRoleName, Permission.READ));
		assertTrue(dao.grantRolePermission(readerRoleName, Permission.READ));
		// grant role on path
		caught = false;
		try {
			dao.grantRoleOnPath("not a user", path, readerRoleName);
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			if(e instanceof NoSuchUserException)
				caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.grantRoleOnPath(username, path, "not a role");
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			if(e instanceof NoSuchRoleException)
				caught = true;
		}
		assertTrue(caught);
		try {
			assertTrue(dao.grantRoleOnPath(username, path, readerRoleName));
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			fail(e.toString());
		}
		try {
			assertTrue(dao.checkPermissionOnPath(username, path, Permission.READ));
		} catch (NoSuchUserException | PathErrorException e) {
			fail(e.toString());
		}
		try {
			assertFalse(dao.grantRoleOnPath(username, path, readerRoleName));
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			fail(e.toString());
		}
		// revoke role on path
		caught = false;
		try {
			dao.revokeRoleOnPath(username, "not a path", readerRoleName);
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			if(e instanceof PathErrorException)
				caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.revokeRoleOnPath("not a user", path, readerRoleName);
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			if(e instanceof NoSuchUserException)
				caught = true;
		}
		assertTrue(caught);
		caught = false;
		try {
			dao.revokeRoleOnPath(username, path, "not a role");
		} catch (PathErrorException | NoSuchUserException | NoSuchRoleException e) {
			if(e instanceof NoSuchRoleException)
				caught = true;
		}
		assertTrue(caught);
		
		assertTrue(dao.revokeRoleOnPath(username, path, readerRoleName));
		assertFalse(dao.revokeRoleOnPath(username, path, readerRoleName));
		assertTrue(dao.grantRoleOnPath(username, path, readerRoleName));

		// grant / revoke permission to role should influence user permission
		assertTrue(dao.grantRolePermission(readerRoleName, Permission.WRITE));
		assertTrue(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.WRITE));
		assertTrue(dao.revokeRolePermission(readerRoleName, Permission.WRITE));
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.WRITE));
		
		// grant / revoke role to path should influence user permission
		String writerRoleName = "writer";
		dao.addRole(writerRoleName);
		dao.grantRolePermission(writerRoleName, Permission.WRITE);
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.WRITE));
		dao.grantRoleOnPath(username, path, writerRoleName);
		assertTrue(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.WRITE));
		dao.revokeRoleOnPath(username, path, readerRoleName);
		assertFalse(dao.checkPermissionOnPath(username, path, Permission.READ | Permission.WRITE));
		assertTrue(dao.checkPermissionOnPath(username, path, Permission.WRITE));
	}
}
