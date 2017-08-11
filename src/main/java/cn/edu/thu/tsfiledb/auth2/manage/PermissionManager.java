package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.exception.PathAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.RoleAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.WrongNodetypeException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeNode;
import cn.edu.thu.tsfiledb.conf.AuthConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.utils.PathUtils;

public class PermissionManager {
	private static AuthConfig authConfig = TsfileDBDescriptor.getInstance().getConfig().authConfig;
	private static Logger logger = LoggerFactory.getLogger(PermissionManager.class);
	private static final int cacheCapacity = authConfig.PERM_CACHE_CAPACITY;
	// <<uid, path>, Set<roleID>>
	private HashMap<Pair<Integer, String>, Set<Integer>> rolesCache = new HashMap<>();
	private LinkedList<Pair<Integer, String>> LRUList = new LinkedList<>();
	private boolean initialized =  false;
	
	private PermissionManager() {

	}
	
	private static class InstanceHolder {
		private static final PermissionManager instance = new PermissionManager();
	}

	public static PermissionManager getInstance() {
		if (!InstanceHolder.instance.initialized) {
			InstanceHolder.instance.init();
		}
		return InstanceHolder.instance;
	}

	private void init() {
		initialized = true;
	}

	/**
	 * Collect the roles granted to user “uid” in “path”
	 * 
	 * @param uid
	 * @param path
	 * @return set of roleIDs
	 * @throws PathErrorException
	 * @throws IOException
	 * @throws WrongNodetypeException
	 */
	public Set<Integer> findRolesOnPath(int uid, String path)
			throws PathErrorException, IOException, WrongNodetypeException {
		Set<Integer> roleSet;
		Pair<Integer, String> index = new Pair<Integer, String>(uid, path);
		roleSet = rolesCache.get(index);
		if (roleSet != null) {
			return roleSet;
		}

		NodeManager nodeManager = NodeManager.getInstance();
		String[] pathLevels = PathUtils.getPathLevels(path);

		PermTreeNode next = nodeManager.getNode(uid, 0);
		roleSet = nodeManager.findRoles(uid, next);
		for (int i = 1; i < pathLevels.length; i++) {
			int nextIndex = nodeManager.findChild(uid, next, pathLevels[i]);
			if (nextIndex == -1) {
				logger.error("{} in {} does not exist", pathLevels[i], path);
				throw new PathErrorException(path + " not exist");
			}
			next = nodeManager.getNode(uid, nextIndex);
			roleSet.addAll(nodeManager.findRoles(uid, next));
		}

		rolesCache.put(index, roleSet);
		LRUList.addFirst(index);
		if (rolesCache.size() > cacheCapacity) {
			index = LRUList.removeLast();
			rolesCache.remove(index);
		}

		return roleSet;
	}

	/**
	 * let user “uid” have role “rid” on “path”
	 * 
	 * @param uid
	 *            user id
	 * @param path
	 *            should start with "root"
	 * @param rid
	 *            role id
	 * @return true if the permission is successfully granted
	 * @throws IOException
	 * @throws PathErrorException
	 * @throws RoleAlreadyExistException
	 * @throws PathAlreadyExistException
	 * @throws WrongNodetypeException
	 */
	public boolean grantRoleOnPath(int uid, String path, int rid) throws IOException, PathErrorException,
			RoleAlreadyExistException, WrongNodetypeException, PathAlreadyExistException {
		NodeManager nodeManager = NodeManager.getInstance();
		PermTreeNode leafNode = nodeManager.getLeaf(uid, path);
		boolean success = nodeManager.addRole(uid, leafNode, rid);
		if (success) {
			Set<Integer> roleSet;
			Pair<Integer, String> index = new Pair<Integer, String>(uid, path);
			roleSet = rolesCache.get(index);
			if (roleSet != null) {
				roleSet.add(rid);
			}
		}
		return success;
	}

	/**
	 * delete user “uid”'s role “rid” on “path”
	 * 
	 * @param uid
	 *            user id
	 * @param path
	 *            should start with "root"
	 * @param rid
	 *            role id
	 * @return true if the role is successfully deleted
	 * @throws PathErrorException
	 * @throws AuthException
	 *             when the role cannot be found
	 * @throws IOException
	 * @throws PathAlreadyExistException
	 * @throws WrongNodetypeException
	 */
	public boolean revokeRoleOnPath(int uid, String path, int rid)
			throws PathErrorException, IOException, WrongNodetypeException, PathAlreadyExistException {
		NodeManager nodeManager = NodeManager.getInstance();
		PermTreeNode leafNode = nodeManager.getLeaf(uid, path);
		boolean success = nodeManager.deleteRole(uid, leafNode, rid);
		if (success) {
			Set<Integer> roleSet;
			Pair<Integer, String> index = new Pair<Integer, String>(uid, path);
			roleSet = rolesCache.get(index);
			if (roleSet != null) {
				roleSet.remove(rid);
			}
		}
		return success;
	}

	/**
	 * check if user “uid” has “permission” on “path”
	 * 
	 * @param uid
	 * @param path
	 * @param permission
	 *            permission to be checked
	 * @return
	 * @throws IOException
	 * @throws PathErrorException
	 * @throws WrongNodetypeException
	 */
	public boolean checkPermissionOnPath(int uid, String path, long permission)
			throws IOException, WrongNodetypeException {
		RoleManager roleManager = RoleManager.getInstance();

		Set<Integer> roleIDs;
		try {
			roleIDs = findRolesOnPath(uid, path);
		} catch (PathErrorException e) {
			return false;
		}
		long userPerm = roleManager.rolesToPermission(roleIDs);
		return Permission.test(userPerm, permission);
	}

	public long getPermissionOnPath(int uid, String path)
			throws IOException, PathErrorException, WrongNodetypeException {
		RoleManager roleManager = RoleManager.getInstance();

		Set<Integer> roleIDs = findRolesOnPath(uid, path);
		long userPerm = roleManager.rolesToPermission(roleIDs);
		return userPerm;
	}
}
