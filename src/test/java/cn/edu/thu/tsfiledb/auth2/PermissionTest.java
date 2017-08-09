package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.manage.NodeManager;
import cn.edu.thu.tsfiledb.auth2.manage.PermissionManager;
import cn.edu.thu.tsfiledb.auth2.manage.RoleManager;
import cn.edu.thu.tsfiledb.auth2.manage.UserManager;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.auth2.model.Role;
import cn.edu.thu.tsfiledb.auth2.model.User;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class PermissionTest {

	@Test
	public void test() throws IOException, PathErrorException, AuthException {
		UserManager userManager = UserManager.getInstance();
		NodeManager nodeManager = NodeManager.getInstance();
		PermissionManager permissionManager = PermissionManager.getInstance();
		RoleManager roleManager = RoleManager.getInstance();
		
		String username = "permissionTest", password = "123456";
		userManager.deleteUser(username);
		userManager.createUser(username, password);
		User user = userManager.findUser(username);
		String readRoleName = "reader";
		roleManager.deleteRole(readRoleName);
		roleManager.createRole(readRoleName);
		Role readerRole = roleManager.findRole(readRoleName);
		String writeRoleName = "writer";
		roleManager.deleteRole(writeRoleName);
		roleManager.createRole(writeRoleName);
		Role writerRole = roleManager.findRole(writeRoleName);
		
		String path1 = "root.a.b.c", path2 = "root.a.b";
		assertTrue(permissionManager.grantRoleOnPath(user.getID(), path1, readerRole.getID()));
		assertTrue(permissionManager.grantRoleOnPath(user.getID(), path2, writerRole.getID()));
		long permission = Permission.READ | Permission.MODIFY;
		assertTrue(permissionManager.checkPermissionOnPath(user.getID(), path1, Permission.NONE));
		assertFalse(permissionManager.checkPermissionOnPath(user.getID(), path1, permission));
		roleManager.grantPermission(readerRole.getRoleName(), Permission.READ);
		roleManager.grantPermission(writerRole.getRoleName(), Permission.MODIFY);
		Set<Integer> roles = permissionManager.findRolesOnPath(user.getID(), path1);

		assertTrue(permissionManager.checkPermissionOnPath(user.getID(), path1, permission));
		assertTrue(permissionManager.checkPermissionOnPath(user.getID(), path2, Permission.MODIFY));
		assertFalse(permissionManager.checkPermissionOnPath(user.getID(), path2, permission));
		
		userManager.deleteUser(username);
	}

}
