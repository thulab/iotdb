package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/** this contains addtional roles (ID) of a path node
 * @author jt
 */
public class RoleContent extends PermTreeContent {
	// except the first 4 bytes for size, the remaining bytes are for entries
	public static final int MAX_CAPACITY = (RECORD_SIZE - 2 * Integer.BYTES) / Integer.BYTES;
	
	private int roleNum = 0;
	private int emptyRoleNum = 0;
	private int[] roles = new int[MAX_CAPACITY];
	
	public RoleContent() {
		
	}
	
	public static PermTreeContent readObject(RandomAccessFile raf) throws IOException {
		byte[] buffer = new byte[RECORD_SIZE];
		raf.readFully(buffer);
		ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
		RoleContent content = new RoleContent();
		content.roleNum = byteBuffer.getInt();
		content.emptyRoleNum = byteBuffer.getInt();
		for(int i = 0; i < MAX_CAPACITY; i++) {
			content.roles[i] = byteBuffer.getInt();
		}
		return content;
	}
	
	public void writeObject(RandomAccessFile raf) throws IOException {
		ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_SIZE);
		byteBuffer.putInt(roleNum);
		byteBuffer.putInt(emptyRoleNum);
		for(int i = 0; i < MAX_CAPACITY; i++) {
			byteBuffer.putInt(roles[i]);
		}
		raf.write(byteBuffer.array());
	}
	
	public boolean addRole(int roleID) {
		if(emptyRoleNum > 0) {
			for(int i = 0; i < roleNum; i++) {
				if(roles[i] == -1) {
					roles[i] = roleID;
					return true;
				}
			}
		} else if(roleNum < MAX_CAPACITY) {
			roles[roleNum++] = roleID;
			return true;
		}
		return false;
	}
	
	public boolean deleteRole(int roleID) {
		for(int i = 0; i < roleNum; i++) {
			if(roles[i] == roleID) {
				roles[i] = -1;
				if(i == roleNum - 1) {
					roleNum--;
				} else {
					emptyRoleNum ++;
				}
				return true;
			}
		}
		return false;
	}

	public int[] getRoles() {
		return roles;
	}

	public void setRoles(int[] roles) {
		this.roles = roles;
	}

	public int getRoleNum() {
		return roleNum;
	}

	public void setRoleNum(int roleNum) {
		this.roleNum = roleNum;
	}

	public int getEmptyRoleNum() {
		return emptyRoleNum;
	}

	public void setEmptyRoleNum(int emptyRoleNum) {
		this.emptyRoleNum = emptyRoleNum;
	}
}
