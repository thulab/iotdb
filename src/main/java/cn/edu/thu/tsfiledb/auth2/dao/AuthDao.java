package cn.edu.thu.tsfiledb.auth2.dao;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchPermException;
import cn.edu.thu.tsfiledb.auth2.exception.NoSuchRoleException;
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

public class AuthDao {
	private static Logger logger = LoggerFactory.getLogger(AuthDao.class);

	NodeManager nodeManager;
	UserManager userManager;
	RoleManager roleManager;
	PermissionManager permManager;

	private static AuthDao instance;

	private AuthDao() {

	}

	public static AuthDao getInstance() throws IOException {
		if (instance == null) {
			instance = new AuthDao();
			instance.init();
		}
		return instance;
	}

	private void init() throws IOException {
		nodeManager = NodeManager.getInstance();
		userManager = UserManager.getInstance();
		roleManager = RoleManager.getInstance();
		permManager = PermissionManager.getInstance();
	}

	/** login for user <username> using <password>
	 * @param username
	 * @param password
	 * @return true if both <username> and <password> are correct
	 * 			false if at least one of them is incorrect
	 */
	public boolean login(String username, String password) {
		boolean success = false;
		success = userManager.authorize(username, password);
		return success;
	}
	
	/**
	 * Add a user by <username> and <password>
	 * 
	 * @param username
	 * @param password
	 * @return true if success, false if the user already exists
	 */
	public boolean addUser(String username, String password) {
		boolean success = false;
		try {
			success = userManager.createUser(username, password);
		} catch (IOException e) {
			logger.error("Create user {} failed because {}", username, e.toString());
		}
		return success;
	}

	public boolean addUser(User user) {
		return addUser(user.getUsername(), user.getPassword());
	}
	
	public User findUser(String username) throws NoSuchUserException {
		User user = userManager.findUser(username);
		if(user == null) {
			logger.info("user {} not exist",username);
			throw new NoSuchUserException(username);
		}
		return user;
	}
	
	/** Delete a user by <username>
	 * @param username
	 * @return true if the user is deleted, false if the user does not exist
	 */
	public boolean deleteUser(String username) {
		boolean success = false;
		try {
			success = userManager.deleteUser(username);
		} catch (IOException e) {
			logger.error("Delete user {} failed because {}", username, e.toString());
		}
		return success;
	}

	/** Add a role by <roleName>
	 * @param roleName
	 * @return true if success, false if the role already exists
	 */
	public boolean addRole(String roleName) {
		boolean success = false;
		try {
			success = roleManager.createRole(roleName);
		} catch (IOException e) {
			logger.error("Create role {} failed because {}", roleName, e.toString());
		}
		return success;
	}
	
	public boolean addRole(Role role) {
		return addRole(role.getRoleName());
	}
	
	/** find a role by <roleName>
	 * @param roleName
	 * @return
	 * @throws NoSuchRoleException
	 */
	public Role findRole(String roleName) throws NoSuchRoleException {
		Role role = roleManager.findRole(roleName);
		if(role == null) {
			logger.info("role {} not exist", roleName);
			throw new NoSuchRoleException(roleName);
		}
		return role;
	}
	
	/** Delete a role <roleName>
	 * @param roleName
	 * @return true if the role is deleted, false if the role does not exist
	 */
	public boolean deleteRole(String roleName) {
		boolean success = false;
		try {
			success = roleManager.deleteRole(roleName);
		} catch (IOException e) {
			logger.error("Delete role {} failed because {}", roleName, e.toString());
		}
		return success;
	}
	
	/** Give user <username> role <roleName> on <path>
	 * @param username
	 * @param path
	 * @param roleName
	 * @return true if the role is correctly added, 
	 * 			false if the role already exists 
	 * @throws PathErrorException
	 * @throws RoleAlreadyExistException
	 * @throws NoSuchUserException 
	 * @throws NoSuchRoleException 
	 */
	public boolean grantRoleOnPath(String username, String path, String roleName) throws PathErrorException, NoSuchUserException, NoSuchRoleException {
		User user = findUser(username);
		Role role = findRole(roleName);
		
		boolean success = false;
		try {
			success = permManager.grantRoleOnPath(user.getID(), path, role.getID());
		} catch (PathErrorException e) {
			logger.info("During grant : path {} is illegal", path);
			throw e;
		} catch (Exception e) {
			logger.error("Grant role failed because : {}", e.toString());
		}
		
		return success;
	}
	
	/** Revoke user <username> role <roleName> on <path>
	 * @param username
	 * @param path
	 * @param roleName
	 * @return true if success, false if the user does not have <roleName> on <path>
	 * @throws PathErrorException 
	 * @throws NoSuchUserException 
	 * @throws NoSuchRoleException 
	 */
	public boolean revokeRoleOnPath(String username, String path, String roleName) throws PathErrorException, NoSuchUserException, NoSuchRoleException {
		User user = findUser(username);
		Role role = findRole(roleName);
		
		boolean success = false;
		try {
			success = permManager.revokeRoleOnPath(user.getID(), path, role.getID());
		} catch (PathErrorException e) {
			logger.info("During grant : path {} is illegal", path);
			throw e;
		} catch (Exception e) {
			logger.error("Grant role failed because : {}", e.toString());
		} 
		return success;
	}
	
	/** Give a role <roleName> <permission>
	 * @param roleName
	 * @param permission
	 * @return true if the permission is correctly given,
	 * 			false if the permission is already given to the role
	 * @throws NoSuchRoleException 
	 */
	public boolean grantRolePermission(String roleName, long permission) throws NoSuchRoleException {
		boolean success = false;
		try {
			success = roleManager.grantPermission(roleName, permission);
		} catch (IOException e) {
			logger.error("Grant permission failed because : {}", e.toString());
		} 
		return success;
	}
	
	/** Revoke a role <roleName> <permission>
	 * @param roleName
	 * @param permission
	 * @return true if the permission is correctly revoked,
	 * 			false if the permission does not belong to the role
	 * @throws NoSuchRoleException 
	 */
	public boolean revokeRolePermission(String roleName, long permission) throws NoSuchRoleException {
		boolean success = false;
		try {
			success = roleManager.revokePermission(roleName, permission);
		} catch (IOException e) {
			logger.error("Revoke permission failed because : {}", e.toString());
		} 
		return success;
	}
	
	/** check whether user <username> has <permission> on <path>
	 * @param username
	 * @param path
	 * @param permission
	 * @return true if the user has such permission, false if the user does not have such permission
	 * @throws NoSuchUserException
	 * @throws PathErrorException
	 */
	public boolean checkPermissionOnPath(String username, String path, long permission) throws NoSuchUserException, PathErrorException {
		User user = findUser(username);
		boolean checked = false;
		try {
			checked = permManager.checkPermissionOnPath(user.getID(), path, permission);
		} catch (PathErrorException e) {
			throw e;
		} catch (Exception e) {
			logger.error("Permission check failed because {}", e.toString());
		}
		return checked;
	}
	
	/** find all permissions of user <username> on <path>
	 * @param username
	 * @param path
	 * @return
	 * @throws NoSuchUserException
	 * @throws PathErrorException
	 */
	public long getPermissionOnPath(String username, String path) throws NoSuchUserException, PathErrorException {
		User user = findUser(username);
		long permission = Permission.NONE;
		try {
			permission = permManager.getPermissionOnPath(user.getID(), path);
		} catch (PathErrorException e) {
			throw e;
		} catch (Exception e) {
			logger.error("Get permission failed because", e.toString());
		}
		return permission;
	}
	
	/** Replace user's <username> password with <newPassword>
	 * @param username
	 * @param newPassword
	 * @return true if modification succeeded, false if IOException is raised
	 * @throws NoSuchUserException
	 */
	public boolean modifyPassword(String username, String newPassword) throws NoSuchUserException {
		boolean success = false;
		try {
			success = userManager.modifyPW(username, newPassword);
		} catch (IOException e) {
			logger.error("Modify password failed because {}", e.toString());
		}
		return success;
	}
	
	public User[] getAllUsers() {
		return userManager.getAllUsers();
	}
	
	public Role[] getAllRoles() {
		return roleManager.getAllRoles();
	}
}
