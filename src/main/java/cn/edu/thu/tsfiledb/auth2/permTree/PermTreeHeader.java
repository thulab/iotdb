package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import cn.edu.thu.tsfiledb.auth2.manage.AuthConfig;
import cn.edu.thu.tsfiledb.utils.SerializeUtils;

public class PermTreeHeader {
	public static final int NORMAL_NODE = 0;
	public static final int SUBNODE_EXTENSION = 1;
	public static final int ROLE_EXTENSION = 2;

	public static final int MAX_NODENAME_LENGTH = AuthConfig.MAX_NODENAME_LENGTH; // in byte
	public static final int RECORD_SIZE = 960;
	public static final int MAX_ROLE_NUM = (RECORD_SIZE - MAX_NODENAME_LENGTH - 7 * Integer.BYTES) / Integer.BYTES;

	private int parentIndex;
	private int currentIndex;
	private int nodeType;
	private String nodeName;
	private int subnodeExtIndex = -1;
	private int roleExtIndex = -1;
	private int roleNum = 0;
	private int emptyRoleNum = 0; // the number of roles deleted but not reused
	private int[] roles = new int[MAX_ROLE_NUM];

	public PermTreeHeader() {
		this.subnodeExtIndex = -1;
		this.roleExtIndex = -1;
		this.roleNum = 0;
		this.emptyRoleNum = 0;
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

	public static PermTreeHeader readObject(RandomAccessFile raf) throws IOException {
		byte[] bytes = new byte[RECORD_SIZE];
		raf.readFully(bytes);
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

		PermTreeHeader header = new PermTreeHeader();
		header.parentIndex = byteBuffer.getInt();
		header.currentIndex = byteBuffer.getInt();
		header.nodeType = byteBuffer.getInt();
		header.nodeName = SerializeUtils.readString(byteBuffer, MAX_NODENAME_LENGTH);
		header.subnodeExtIndex = byteBuffer.getInt();
		header.roleExtIndex = byteBuffer.getInt();
		header.roleNum = byteBuffer.getInt();
		header.setEmptyRoleNum(byteBuffer.getInt());
		for (int i = 0; i < header.roles.length; i++)
			header.roles[i] = byteBuffer.getInt();
		return header;
	}

	public void writeObject(RandomAccessFile raf) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_SIZE);
		buffer.putInt(parentIndex);
		buffer.putInt(currentIndex);
		buffer.putInt(nodeType);
		buffer.put(SerializeUtils.strToBytes(nodeName, MAX_NODENAME_LENGTH));
		buffer.putInt(subnodeExtIndex);
		buffer.putInt(roleExtIndex);
		buffer.putInt(roleNum);
		buffer.putInt(getEmptyRoleNum());
		for (int i = 0; i < roles.length; i++)
			buffer.putInt(roles[i]);
		raf.write(buffer.array());
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
