package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import cn.edu.thu.tsfiledb.utils.SerializeUtils;

/**
 * this contains pointers to subnodes, used for normal nodes and
 * subnode-extension
 * 
 * @author jt
 *
 */
public class SubnodeContent extends PermTreeContent {
	public static final int MAX_NODENAME_LENGTH = PermTreeHeader.MAX_NODENAME_LENGTH;
	// entry is a <subnodeName, subnodeIndex> pair
	public static final int ENTRY_SIZE = MAX_NODENAME_LENGTH + Integer.BYTES;
	// except the first 4 bytes for size, the remaining bytes are for entries
	public static final int MAX_CAPACITY = (RECORD_SIZE - 2 * Integer.BYTES) / ENTRY_SIZE;

	private int size = 0;
	private int emptyNum = 0;
	private String[] subnodeNames = new String[MAX_CAPACITY];
	private int[] subnodeIndex = new int[MAX_CAPACITY];

	public SubnodeContent() {

	}

	public int getSize() {
		return size;
	}

	public static PermTreeContent initRootContent() {
		SubnodeContent content = new SubnodeContent();
		content.size = 0;
		content.emptyNum = 0;
		return content;
	}

	public static PermTreeContent readObject(RandomAccessFile raf) throws IOException {
		byte[] bytes = new byte[RECORD_SIZE];
		raf.readFully(bytes);
		ByteBuffer buffer = ByteBuffer.wrap(bytes);

		SubnodeContent content = new SubnodeContent();
		content.size = buffer.getInt();
		content.emptyNum = buffer.getInt();
		for (int i = 0; i < MAX_CAPACITY; i++) {
			byte[] strBuffer = new byte[MAX_NODENAME_LENGTH];
			buffer.get(strBuffer, 0, MAX_NODENAME_LENGTH);
			content.getSubnodeNames()[i] = SerializeUtils.bytesToStr(strBuffer);
			content.getSubnodeIndex()[i] = buffer.getInt();
		}
		return content;
	}

	public void writeObject(RandomAccessFile raf) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(RECORD_SIZE);
		buffer.putInt(size);
		buffer.putInt(emptyNum);
		for (int i = 0; i < MAX_CAPACITY; i++) {
			buffer.put(SerializeUtils.strToBytes(getSubnodeNames()[i], MAX_NODENAME_LENGTH));
			buffer.putInt(getSubnodeIndex()[i]);
		}
		raf.write(buffer.array());
	}

	public boolean addChild(String childName, int childIndex) {
		if (emptyNum > 0) {
			for (int i = 0; i < size; i++) {
				if (subnodeNames[i].equals("")) {
					subnodeNames[i] = childName;
					subnodeIndex[i] = childIndex;
					return true;
				}
			}
		} else if (size < MAX_CAPACITY) {
			subnodeNames[size] = childName;
			subnodeIndex[size++] = childIndex;
			return true;
		}
		return false;
	}

	public boolean deleteChild(String childName) {
		for (int i = 0; i < size; i++) {
			if (subnodeNames[i].equals(childName)) {
				subnodeNames[i] = "";
				if (i == size - 1) {
					size--;
				} else {
					emptyNum++;
				}
				return true;
			}
		}
		return false;
	}

	public String[] getSubnodeNames() {
		return subnodeNames;
	}

	public void setSubnodeNames(String[] subnodeNames) {
		this.subnodeNames = subnodeNames;
	}

	public int[] getSubnodeIndex() {
		return subnodeIndex;
	}

	public void setSubnodeIndex(int[] subnodeIndex) {
		this.subnodeIndex = subnodeIndex;
	}
}
