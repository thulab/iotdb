package cn.edu.thu.tsfiledb.auth2.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchUserException;
import cn.edu.thu.tsfiledb.auth2.exception.RoleAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.manage.NodeManager;
import cn.edu.thu.tsfiledb.auth2.manage.PermissionManager;
import cn.edu.thu.tsfiledb.auth2.manage.RoleManager;
import cn.edu.thu.tsfiledb.auth2.manage.UserManager;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.auth2.model.Role;
import cn.edu.thu.tsfiledb.auth2.model.User;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

/** This class provide interfaces to the Authority Database.
 *  Users, roles and permissions should be accessed from this class.
 * @author jt
 *
 */
public class AuthDao {
	private static Logger logger = LoggerFactory.getLogger(AuthDao.class);

	NodeManager nodeManager;
	UserManager userManager;
	RoleManager roleManager;
	PermissionManager permManager;

	private static AuthDao instance;

	private AuthDao() {

	}

	public static AuthDao getInstance() throws AuthException {
		if (instance == null) {
			instance = new AuthDao();
			instance.init();
		}
		return instance;
	}

	private void init() throws AuthException {
		try {
			nodeManager = NodeManager.getInstance();
			userManager = UserManager.getInstance();
			roleManager = RoleManager.getInstance();
			permManager = PermissionManager.getInstance();
		} catch (Exception e) {
			logger.error("init AuthDao failed because {}", e.toString());
			throw new AuthException(e.toString());
		}
	}

	/**
	 * login for user "username" using "password"
	 * 
	 * @param username
	 * @param password
	 * @return true if both "username" and "password" are correct throw exception if
	 *         at least one of them is incorrect
	 * @throws AuthException
	 */
	public boolean login(String username, String password) throws AuthException {
		boolean success = false;
		success = userManager.authorize(username, password);
		if (!success) {
			throw new AuthException("username or password is not correct");
		}
		return success;
	}

	/**
	 * Add a user by "username" and "password"
	 * 
	 * @param username
	 * @param password
	 * @return true if success, throw exception if the user already exists
	 * @throws AuthException
	 */
	public boolean addUser(String username, String password) throws AuthException {
		boolean success = false;
		try {
			success = userManager.createUser(username, password);
		} catch (IOException e) {
			logger.error("Create user {} failed because {}", username, e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException("user " + username + " already exists");
		}
		return success;
	}

	public boolean addUser(User user) throws AuthException {
		return addUser(user.getUsername(), user.getPassword());
	}

	/** find a user by "username"
	 * @param username
	 * @return
	 * @throws AuthException
	 * 			when the user cannot be found
	 */
	public User findUser(String username) throws AuthException {
		User user = userManager.findUser(username);
		if (user == null) {
			logger.info("user {} not exist", username);
			throw new AuthException(username + " does not exist");
		}
		return user;
	}

	/**
	 * Delete a user by "username"
	 * 
	 * @param username
	 * @return true if the user is deleted, throw exception if the user does not
	 *         exist
	 * @throws AuthException
	 */
	public boolean deleteUser(String username) throws AuthException {
		boolean success = false;
		try {
			success = userManager.deleteUser(username);
		} catch (IOException e) {
			logger.error("Delete user {} failed because {}", username, e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException("user " + username + " does not exist");
		}
		return success;
	}

	/**
	 * Add a role by "roleName"
	 * 
	 * @param roleName
	 * @return true if success, throw exception if the role already exists
	 * @throws AuthException
	 */
	public boolean addRole(String roleName) throws AuthException {
		boolean success = false;
		try {
			success = roleManager.createRole(roleName);
		} catch (IOException e) {
			logger.error("Create role {} failed because {}", roleName, e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException("role " + roleName + " already exists");
		}
		return success;
	}

	public boolean addRole(Role role) throws AuthException {
		return addRole(role.getRoleName());
	}

	/**
	 * find a role by "roleName"
	 * 
	 * @param roleName
	 * @return throw exception if the role does not exist
	 * @throws AuthException
	 */
	public Role findRole(String roleName) throws AuthException {
		Role role = roleManager.findRole(roleName);
		if (role == null) {
			logger.info("role {} not exist", roleName);
			throw new AuthException(roleName + " does not exist");
		}
		return role;
	}

	/**
	 * Delete a role "roleName"
	 * 
	 * @param roleName
	 * @return true if the role is deleted, throw exception if the role does not
	 *         exist
	 * @throws AuthException
	 */
	public boolean deleteRole(String roleName) throws AuthException {
		boolean success = false;
		try {
			success = roleManager.deleteRole(roleName);
		} catch (IOException e) {
			logger.error("Delete role {} failed because {}", roleName, e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException("role " + roleName + " does not exist");
		}
		return success;
	}

	/**
	 * Give user "username" role "roleName" on "path"
	 * 
	 * @param username
	 * @param path
	 * @param roleName
	 * @return true if the role is correctly added, throw exception if the role
	 *         already exists
	 * @throws PathErrorException
	 * @throws AuthException
	 * @throws RoleAlreadyExistException
	 */
	public boolean grantRoleOnPath(String username, String path, String roleName) throws AuthException {
		User user = findUser(username);
		Role role = findRole(roleName);

		boolean success = false;
		try {
			success = permManager.grantRoleOnPath(user.getID(), path, role.getID());
		} catch (PathErrorException e) {
			logger.info("During grant : path {} is illegal", path);
			throw new AuthException(e.toString());
		} catch (Exception e) {
			logger.error("Grant role failed because : {}", e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException("user " + username + " already has role " + roleName + " on " + path);
		}
		return success;
	}

	/**
	 * Revoke user "username" role "roleName" on "path"
	 * 
	 * @param username
	 * @param path
	 * @param roleName
	 * @return true if success, throw exception the user does not have <roleName> on
	 *         <path>
	 * @throws PathErrorException
	 * @throws AuthException
	 */
	public boolean revokeRoleOnPath(String username, String path, String roleName) throws AuthException {
		User user = findUser(username);
		Role role = findRole(roleName);

		boolean success = false;
		try {
			success = permManager.revokeRoleOnPath(user.getID(), path, role.getID());
		} catch (PathErrorException e) {
			logger.info("During grant : path {} is illegal", path);
			throw new AuthException(e.toString());
		} catch (Exception e) {
			logger.error("Grant role failed because : {}", e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException("user " + username + " does not have role " + roleName + " on " + path);
		}
		return success;
	}

	/**
	 * Give a role "roleName" "permission"
	 * 
	 * @param roleName
	 * @param permission
	 * @return true if the permission is correctly given, throw exception if the
	 *         permission is already given to the role
	 * @throws AuthException
	 */
	public boolean grantRolePermission(String roleName, long permission) throws AuthException {
		boolean success = false;
		try {
			success = roleManager.grantPermission(roleName, permission);
		} catch (IOException e) {
			logger.error("Grant permission failed because : {}", e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException(
					"role " + roleName + " already has permission " + Permission.longToName(permission));
		}
		return success;
	}

	/**
	 * Revoke a role "roleName" "permission"
	 * 
	 * @param roleName
	 * @param permission
	 * @return true if the permission is correctly revoked, throw exception if the
	 *         permission does not belong to the role
	 * @throws AuthException
	 */
	public boolean revokeRolePermission(String roleName, long permission) throws AuthException {
		boolean success = false;
		try {
			success = roleManager.revokePermission(roleName, permission);
		} catch (IOException e) {
			logger.error("Revoke permission failed because : {}", e.toString());
			throw new AuthException(e.toString());
		}
		if (!success) {
			throw new AuthException(
					"role " + roleName + " does not have permission " + Permission.longToName(permission));
		}
		return success;
	}

	/**
	 * check whether user "username" has "permission" on "path"
	 * 
	 * @param username
	 * @param path
	 * @param permission
	 * @return true if the user has such permission, false if the user does not have
	 *         such permission
	 * @throws AuthException
	 */
	public boolean checkPermissionOnPath(String username, String path, long permission) throws AuthException {
		User user = findUser(username);
		boolean checked = false;
		try {
			checked = permManager.checkPermissionOnPath(user.getID(), path, permission);
		} catch (Exception e) {
			logger.error("Permission check failed because {}", e.toString());
			throw new AuthException(e.toString());
		}
		return checked;
	}

	/**
	 * find all permissions of user "username" on "path"
	 * 
	 * @param username
	 * @param path
	 * @return
	 * @throws AuthException
	 */
	public long getPermissionOnPath(String username, String path) throws AuthException {
		User user = findUser(username);
		long permission = Permission.NONE;
		try {
			permission = permManager.getPermissionOnPath(user.getID(), path);
		} catch (PathErrorException e) {
			throw new AuthException(e.toString());
		} catch (Exception e) {
			logger.error("Get permission failed because", e.toString());
			throw new AuthException(e.toString());
		}
		return permission;
	}

	/**
	 * Replace user's "username" password with "newPassword"
	 * 
	 * @param username
	 * @param newPassword
	 * @return true if modification succeeded, false if IOException is raised
	 * @throws NoSuchUserException
	 */
	public boolean modifyPassword(String username, String newPassword) throws AuthException {
		boolean success = false;
		try {
			success = userManager.modifyPW(username, newPassword);
		} catch (IOException e) {
			logger.error("Modify password failed because {}", e.toString());
			throw new AuthException(e.toString());
		}
		return success;
	}

	public User[] getAllUsers() {
		return userManager.getAllUsers();
	}

	public Role[] getAllRoles() {
		return roleManager.getAllRoles();
	}

	/**
	 * find all roles of user "username" on "path"
	 * 
	 * @param username
	 * @param path
	 * @return list of role names
	 * @throws AuthException
	 */
	public Object getRolesOnPath(String username, String fullPath) throws AuthException {
		User user = findUser(username);
		Set<Integer> roleIDs = null;
		try {
			roleIDs = permManager.findRolesOnPath(user.getID(), fullPath);
		} catch (PathErrorException | IOException e) {
			throw new AuthException(e.toString());
		}

		List<Role> roles = new ArrayList<>();
		if (roleIDs != null) {
			for (Integer roleID : roleIDs) {
				Role role = roleManager.findRole(roleID);
				if (role != null) {
					roles.add(role);
				}
			}
		}
		return roles;
	}
}
