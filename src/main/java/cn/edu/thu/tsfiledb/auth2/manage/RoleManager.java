package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchRoleException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.auth2.model.Role;
import cn.edu.thu.tsfiledb.auth2.model.Rolemeta;
import cn.edu.thu.tsfiledb.auth2.model.User;
import cn.edu.thu.tsfiledb.conf.AuthConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class RoleManager {
	private static AuthConfig authConfig = TsfileDBDescriptor.getInstance().getConfig().authConfig;
	private static Logger logger = LoggerFactory.getLogger(RoleManager.class);

	private static String roleFolder = authConfig.ROLE_FOLDER;
	private static String roleInfoFile = authConfig.ROLE_INFO_FILE;

	private HashMap<String, Role> roleNameMap = new HashMap<>();
	private HashMap<Integer, Role> roleIDMap = new HashMap<>();
	
	private boolean initialized = false;
	
	private static class InstanceHolder {
		private static final RoleManager instance = new RoleManager();
	}

	private RoleManager() {

	}

	public static RoleManager getInstance() throws IOException {
		if (!InstanceHolder.instance.initialized) {
			InstanceHolder.instance.init();
		}
		return InstanceHolder.instance;
	}

	private void init() throws IOException {
		File roleFolderFile = new File(roleFolder);
		File infoFile = new File(roleFolder + roleInfoFile);
		if (!roleFolderFile.exists())
			roleFolderFile.mkdirs();
		if (!infoFile.exists())
			infoFile.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(infoFile, "r");
		while (raf.getFilePointer() + User.RECORD_SIZE < raf.length()) {
			Role role = Role.readObject(raf);
			if (!role.getRoleName().equals("")) {
				roleNameMap.put(role.getRoleName(), role);
				roleIDMap.put(role.getID(), role);
			}
		}
		raf.close();
		initialized = true;
	}

	public Role findRole(int roleID) {
		return roleIDMap.get(roleID);
	}

	public Role findRole(String roleName) {
		return roleNameMap.get(roleName);
	}

	synchronized public boolean createRole(String roleName) throws IOException {
		if (roleNameMap.get(roleName) != null) {
			return false;
		}
		Rolemeta rolemeta = Rolemeta.getInstance();
		int rid = rolemeta.getMaxRID();
		Role role = new Role(roleName, rid);
		flushRole(role);
		rolemeta.increaseMaxRID();

		roleNameMap.put(roleName, role);
		roleIDMap.put(rid, role);
		return true;
	}

	private void flushRole(Role role) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(roleFolder + roleInfoFile, "rw");
		raf.seek(role.getID() * Role.RECORD_SIZE);
		role.writeObject(raf);
		raf.close();
	}

	public boolean deleteRole(String roleName) throws IOException {
		Role role = roleNameMap.get(roleName);
		if (role == null) {
			logger.warn("Attemp to delete non-exist role {}", roleName);
			return false;
		}
		Role blankRole = new Role("", role.getID());
		flushRole(blankRole);
		roleNameMap.remove(roleName);
		roleIDMap.remove(role.getID());
		return true;
	}

	public boolean grantPermission(String roleName, long permission) throws IOException, NoSuchRoleException {
		Role role = roleNameMap.get(roleName);
		if (role == null) {
			throw new NoSuchRoleException(roleName);
		}
		if (Permission.test(role.getPermission(), permission)) {
			return false;
		}
		role.setPermission(Permission.combine(role.getPermission(), permission));
		flushRole(role);
		return true;
	}

	public boolean revokePermission(String roleName, long permission) throws IOException, NoSuchRoleException {
		Role role = roleNameMap.get(roleName);
		if (role == null) {
			throw new NoSuchRoleException(roleName);
		}
		long oldPermission = role.getPermission();
		if (!Permission.test(oldPermission, permission)) {
			logger.error("Role {} has no permission {}. It has {}", roleName, Permission.longToName(permission),
					Permission.longToName(role.getPermission()));
			return false;
		}
		role.setPermission(Permission.revoke(oldPermission, permission));
		flushRole(role);
		return true;
	}

	public long rolesToPermission(Set<Integer> roleIDs) {
		Integer[] IDArray = roleIDs.toArray(new Integer[0]);
		long permission = 0l;
		for (int i = 0; i < IDArray.length; i++) {
			Role role = roleIDMap.get(IDArray[i]);
			if (role != null) {
				permission = Permission.combine(permission, role.getPermission());
			} else {
				logger.warn("cannot find role whose ID is {}", IDArray[i]);
			}
		}
		return permission;
	}

	public Role[] getAllRoles() {
		return roleIDMap.values().toArray(new Role[0]);
	}
}
