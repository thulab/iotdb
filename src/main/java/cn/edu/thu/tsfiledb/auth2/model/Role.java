package cn.edu.thu.tsfiledb.auth2.model;

import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.auth2.exception.ReadObjectException;
import cn.edu.thu.tsfiledb.auth2.exception.WriteObjectException;
import cn.edu.thu.tsfiledb.utils.SerializeUtils;

public class Role {
	public static final int MAX_NAME_LENGTH = 256;
	public static final int RECORD_SIZE = MAX_NAME_LENGTH + Integer.BYTES + Long.BYTES;
	
	private int ID;
	private String roleName;
	private long permission;
	
	public Role() {
		
	}
	
	public Role(String roleName) {
		this.roleName = roleName;
	}
	
	public Role(String roleName, int ID) {
		this.roleName = roleName;
		this.ID = ID;
	}
	
	public static Role readObject(RandomAccessFile raf) throws IOException, ReadObjectException {
		Role role = new Role();
		role.setID(raf.readInt());
		
		role.setRoleName(SerializeUtils.readString(raf, MAX_NAME_LENGTH));
		
		role.setPermission(raf.readLong());
		return role;
	}
	
	public void writeObject(RandomAccessFile raf) throws IOException, WriteObjectException {
		raf.writeInt(ID);
		
		SerializeUtils.writeString(raf, roleName, MAX_NAME_LENGTH);
		
		raf.writeLong(permission);
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
