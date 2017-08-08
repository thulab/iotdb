package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import cn.edu.thu.tsfiledb.utils.SerializeUtils;

public class PermTreeHeader {
	public static final int NORMAL_NODE = 0;
	public static final int SUBNODE_EXTENSION = 1;
	public static final int ROLE_EXTENSION = 2;

	public static final int MAX_NODENAME_LENGTH = 256;
	public static final int MAX_ROLE_NUM = 170;
	public static final int RECORD_SIZE = MAX_NODENAME_LENGTH + Integer.BYTES * (7 + MAX_ROLE_NUM); // should be 960B

	private int parentIndex;
	private int currentIndex;
	private int nodeType;
	private String nodeName;
	private int subnodeExtIndex = -1;
	private int roleExtIndex = -1;
	private int roleNum = 0;
	private int emptyRoleNum = 0;			// the number of roles deleted but not reused
	private int[] roles = new int[MAX_ROLE_NUM];
	
	public PermTreeHeader() {
		
	}
	
	public static PermTreeHeader readObject(RandomAccessFile raf) throws IOException {
		PermTreeHeader header = new PermTreeHeader();
		header.parentIndex = raf.readInt();
		header.currentIndex = raf.readInt();
		header.nodeType = raf.readInt();
		header.nodeName = SerializeUtils.readString(raf, MAX_NODENAME_LENGTH);
		header.subnodeExtIndex = raf.readInt();
		header.roleExtIndex = raf.readInt();
		header.roleNum = raf.readInt();
		header.setEmptyRoleNum(raf.readInt());
		for (int i = 0; i < header.roles.length; i++)
			header.roles[i] = raf.readInt();
		return header;
	}
	
	public static PermTreeHeader initRootHeader() {
		PermTreeHeader header = new PermTreeHeader();
		header.parentIndex = -1;
		header.currentIndex = 0;
		header.nodeType = NORMAL_NODE;
		header.nodeName = "root";
		header.subnodeExtIndex = -1;
		header.roleExtIndex = -1;
		header.roleNum = 0;
		header.emptyRoleNum = 0;
		return header;
	}

	public void writeObject(RandomAccessFile raf) throws IOException {
		raf.writeInt(parentIndex);
		raf.writeInt(currentIndex);
		raf.writeInt(nodeType);
		SerializeUtils.writeString(raf, nodeName, MAX_NODENAME_LENGTH);
		raf.writeInt(subnodeExtIndex);
		raf.writeInt(roleExtIndex);
		raf.writeInt(roleNum);
		raf.writeInt(getEmptyRoleNum());
		for (int i = 0; i < roles.length; i++)
			raf.writeInt(roles[i]);
	}
	
	public int getParentIndex() {
		return parentIndex;
	}

	public void setParentIndex(int parentIndex) {
		this.parentIndex = parentIndex;
	}

	public int getCurrentIndex() {
		return currentIndex;
	}

	public void setCurrentIndex(int currentIndex) {
		this.currentIndex = currentIndex;
	}

	public int getNodeType() {
		return nodeType;
	}

	public void setNodeType(int nodeType) {
		this.nodeType = nodeType;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public int getSubnodeExtIndex() {
		return subnodeExtIndex;
	}

	public void setSubnodeExtIndex(int subnodeExtIndex) {
		this.subnodeExtIndex = subnodeExtIndex;
	}

	public int getRoleExtIndex() {
		return roleExtIndex;
	}

	public void setRoleExtIndex(int roleExtIndex) {
		this.roleExtIndex = roleExtIndex;
	}

	public int getRoleNum() {
		return roleNum;
	}

	public void setRoleNum(int roleNum) {
		this.roleNum = roleNum;
	}

	public int[] getRoles() {
		return roles;
	}

	public void setRoles(int[] roles) {
		this.roles = roles;
	}

	public int getEmptyRoleNum() {
		return emptyRoleNum;
	}

	public void setEmptyRoleNum(int emptyRoleNum) {
		this.emptyRoleNum = emptyRoleNum;
	}
}
