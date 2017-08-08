package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchPermException;
import cn.edu.thu.tsfiledb.auth2.model.Permission;
import cn.edu.thu.tsfiledb.auth2.model.Role;
import cn.edu.thu.tsfiledb.auth2.model.Rolemeta;
import cn.edu.thu.tsfiledb.auth2.model.User;

public class RoleManager {
	private static Logger logger = LoggerFactory.getLogger(RoleManager.class);
	private static RoleManager instance;
	
	private static String roleFolder = Rolemeta.getRoleFolder();
	private static String roleInfoFile = "roleInfo";
	
	private HashMap<String, Role> roles = new HashMap<>();
	
	private RoleManager() {
		
	}
	
	public static RoleManager getInstance() throws IOException {
		if(instance == null) {
			instance = new RoleManager();
			instance.init();
		}
		return instance;
	}
	
	private void init() throws IOException {
		File roleFolderFile = new File(roleFolder);
		File infoFile = new File(roleFolder + roleInfoFile);
		if(!roleFolderFile.exists())
			roleFolderFile.mkdirs();
		if(!infoFile.exists())
			infoFile.createNewFile();
		
		RandomAccessFile raf = new RandomAccessFile(infoFile, "r");
		while(raf.getFilePointer() + User.RECORD_SIZE < raf.length()) {
			Role role = Role.readObject(raf);
			if(!role.getRoleName().equals(""))
				roles.put(role.getRoleName(), role);
		}
		raf.close();
	}
	
	public Role findRole(String roleName) {
		return roles.get(roleName);
	}
	
	public boolean createRole(String roleName) throws IOException {
		if(roles.get(roleName) != null) {
			return false;
		}
		Rolemeta rolemeta = Rolemeta.getInstance();
		int rid = rolemeta.getMaxRID();
		Role role = new Role(roleName, rid);
		flushRole(role);
		rolemeta.increaseMaxRID();
		
		roles.put(roleName, role);
		return true;
	}
	
	private void flushRole(Role role) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(roleFolder + roleInfoFile, "rw");
		raf.seek(role.getID() * Role.RECORD_SIZE);
		role.writeObject(raf);
		raf.close();
	}
	
	public boolean deleteRole(String roleName) throws IOException {
		Role role = roles.get(roleName);
		if(role == null) {
			logger.warn("Attemp to delete non-exist role {}",roleName);
			return false;
		}
		Role blankRole = new Role("", role.getID());
		flushRole(blankRole);
		roles.remove(roleName);
		return true;
	}
	
	public boolean grantPermission(String roleName, long permission) throws IOException {
		Role role = roles.get(roleName);
		if(role == null) {
			logger.warn("Attemp to grant permission for non-exist role {}",roleName);
			return false;
		}
		role.setPermission(Permission.combine(role.getPermission(), permission));
		flushRole(role);
		return true;
	}
	
	public boolean revokePermission(String roleName, long permission) throws NoSuchPermException, IOException {
		Role role = roles.get(roleName);
		if(role == null) {
			logger.warn("Attemp to revoke permission for non-exist role {}",roleName);
			return false;
		}
		long oldPermission = role.getPermission();
		if(!Permission.test(oldPermission, permission)) {
			logger.error("Role {} has no permission {}. It has {}",
					roleName, Permission.longToName(permission), Permission.longToName(role.getPermission()));
			throw new NoSuchPermException();
		}
		role.setPermission(Permission.revoke(oldPermission, permission));
		flushRole(role);
		return true;
	}
}
