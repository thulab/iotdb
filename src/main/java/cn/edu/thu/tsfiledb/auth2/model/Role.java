package cn.edu.thu.tsfiledb.auth2.model;

public class Role {
	private int ID;
	private String roleName;
	private long permission;
	
	public Role() {
		
	}
	
	public Role(String roleName) {
		this.roleName = roleName;
	}
	
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	public String getRoleName() {
		return roleName;
	}
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
	public long getPermission() {
		return permission;
	}
	public void setPermission(long permission) {
		this.permission = permission;
	}
}
