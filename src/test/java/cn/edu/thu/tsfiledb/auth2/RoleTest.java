package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchPermException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchRoleException;
import cn.edu.thu.tsfiledb.auth2.manage.RoleManager;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.auth2.model.Role;

public class RoleTest {

	@Test
	public void createTest() throws IOException {
		RoleManager roleManager = RoleManager.getInstance();
		String roleName = "newRole";
		roleManager.createRole(roleName);
		
		Role role = roleManager.findRole(roleName);
		assertTrue(role != null);
		assertTrue(role.getRoleName().equals(roleName));
	}
	
	@Test
	public void deleteTest() throws IOException {
		RoleManager roleManager = RoleManager.getInstance();
		String roleName = "newRole-delete";
		roleManager.createRole(roleName);
		
		assertTrue(roleManager.deleteRole(roleName));
		Role role = roleManager.findRole(roleName);
		assertTrue(role == null);
		assertTrue(roleManager.createRole(roleName));
	}
	
	@Test
	public void rolePermissionTest() throws IOException, NoSuchPermException, NoSuchRoleException {
		RoleManager roleManager = RoleManager.getInstance();
		String roleName = "newRole-permission";
		roleManager.deleteRole(roleName);
		roleManager.createRole(roleName);
		
		// grant
		assertTrue(roleManager.grantPermission(roleName, Permission.READ));
		assertTrue(roleManager.grantPermission(roleName, Permission.MODIFY));
		Role role = roleManager.findRole(roleName);
		long perm = Permission.combine(Permission.READ, Permission.MODIFY);
		assertEquals(role.getPermission(), perm);
		boolean caught = false;
		try {
			assertFalse(roleManager.grantPermission(roleName + "dummy", Permission.READ));
		} catch (Exception e1) {
			caught = true;
		}
		assertTrue(caught);
		
		// revoke
		assertTrue(roleManager.revokePermission(roleName, Permission.READ));
		role = roleManager.findRole(roleName);
		assertEquals(role.getPermission(), Permission.MODIFY);
		caught = false;
		try {
			roleManager.revokePermission(roleName + "dummy", Permission.READ);
		} catch (Exception e) {
			caught = true;
		}
		assertTrue(caught);
	}

}
