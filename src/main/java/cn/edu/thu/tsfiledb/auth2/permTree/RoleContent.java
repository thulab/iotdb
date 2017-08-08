package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;

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
		RoleContent content = new RoleContent();
		content.setRoleNum(raf.readInt());
		content.setEmptyRoleNum(raf.readInt());
		for(int i = 0; i < MAX_CAPACITY; i++) {
			content.getRoles()[i] = raf.readInt();
		}
		return content;
	}
	
	public void writeObject(RandomAccessFile raf) throws IOException {
		raf.writeInt(getRoleNum());
		raf.writeInt(getEmptyRoleNum());
		for(int i = 0; i < MAX_CAPACITY; i++)
			raf.writeInt(getRoles()[i]);
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
