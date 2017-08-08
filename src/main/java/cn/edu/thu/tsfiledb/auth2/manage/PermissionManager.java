package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.exception.WrongNodetypeException;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeNode;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.utils.PathUtils;

public class PermissionManager {
	private static Logger logger = LoggerFactory.getLogger(PermissionManager.class);
	private static PermissionManager instance;
	// <<uid, path>, Set<roleID>>
	private HashMap<Pair<Integer, String>, Set<Integer>> rolesCache = new HashMap<>();
	private LinkedList<Pair<Integer, String>> LRUList = new LinkedList<>();
	private int cacheCapacity = 1000;
	
	private PermissionManager() {
	
	}
	
	public static PermissionManager getInstance() {
		if(instance == null) {
			instance = new PermissionManager();
			instance.init();
		}
		return instance;
	}
	
	private void init() {
		
	}
	
	/** Collect the roles granted to user <uid> in <path> 
	 * @param uid
	 * @param path
	 * @return set of roleIDs
	 * @throws PathErrorException
	 * @throws IOException
	 * @throws WrongNodetypeException
	 */
	public Set<Integer> findRolesOfPath(int uid, String path) throws PathErrorException, IOException, WrongNodetypeException {
		Set<Integer> roleSet;
		Pair<Integer, String> index = new Pair<Integer, String>(uid, path);
		roleSet = rolesCache.get(index);
		if(roleSet != null) {
			return roleSet;
		}
		
		NodeManager nodeManager = NodeManager.getInstance();
		String[] pathLevels = PathUtils.getPathLevels(path);
		
		PermTreeNode next = nodeManager.getNode(uid, 0);
		roleSet = nodeManager.findRoles(uid, next);
		for(int i = 1; i < pathLevels.length; i++) {
			int nextIndex = nodeManager.findChild(uid, next, pathLevels[i]);
			if(nextIndex == -1) {
				logger.error("{} in {} does not exist", pathLevels[i], path);
				throw new PathErrorException(path + " not exist");
			}
			next = nodeManager.getNode(uid, nextIndex);
			roleSet.addAll(roleSet);
		}
		
		rolesCache.put(index, roleSet);
		LRUList.addFirst(index);
		if(rolesCache.size() > cacheCapacity) {
			index = LRUList.removeLast();
			rolesCache.remove(index);
		}
		
		return roleSet;
	}
	
	public boolean grantRoleOnPath(int uid, String path, int rid) throws IOException, AuthException, PathErrorException {
		NodeManager nodeManager = NodeManager.getInstance();
		PermTreeNode leafNode = nodeManager.getLeaf(uid, path);
		boolean success = nodeManager.addRole(uid, leafNode, rid);
		if(success) {
			Set<Integer> roleSet;
			Pair<Integer, String> index = new Pair<Integer, String>(uid, path);
			roleSet = rolesCache.get(index);
			if(roleSet != null) {
				roleSet.add(rid);
			}
		}
		return success;
	}
	
	public boolean revokeRoleOnPath(int uid, String path, int rid) throws PathErrorException, AuthException, IOException {
		NodeManager nodeManager = NodeManager.getInstance();
		PermTreeNode leafNode = nodeManager.getLeaf(uid, path);
		boolean success = nodeManager.deleteRole(uid, leafNode, rid);
		if(success) {
			Set<Integer> roleSet;
			Pair<Integer, String> index = new Pair<Integer, String>(uid, path);
			roleSet = rolesCache.get(index);
			if(roleSet != null) {
				roleSet.remove(rid);
			}
		}
		return success;
	}
	
}
