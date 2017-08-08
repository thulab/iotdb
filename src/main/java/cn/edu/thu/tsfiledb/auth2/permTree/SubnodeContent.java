package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfiledb.utils.SerializeUtils;

/** this contains pointers to subnodes, used for normal nodes and subnode-extension
 * @author jt
 *
 */
public class SubnodeContent extends PermTreeContent {
	public static final int MAX_NODENAME_LENGTH = PermTreeHeader.MAX_NODENAME_LENGTH;
	// entry is a <subnodeName, subnodeIndex> pair
	public static final int ENTRY_SIZE = MAX_NODENAME_LENGTH + Integer.BYTES;
	// except the first 4 bytes for size, the remaining bytes are for entries
	public static final int MAX_CAPACITY = (RECORD_SIZE - Integer.BYTES) / ENTRY_SIZE;
	
	private int size = 0;
	private String[] subnodeNames = new String[MAX_CAPACITY];
	private int[] subnodeIndex = new int[MAX_CAPACITY];
	
	public SubnodeContent() {
		
	}
	
	public int getSize() {
		return size;
	}
	
	public static PermTreeContent readObject(RandomAccessFile raf) throws IOException {
		SubnodeContent content = new SubnodeContent();
		content.size = raf.readInt();
		for(int i = 0; i < MAX_CAPACITY; i++) {
			content.getSubnodeNames()[i] = SerializeUtils.readString(raf, MAX_NODENAME_LENGTH);
			content.getSubnodeIndex()[i] = raf.readInt();
		}
		return content;
	}
	
	public static PermTreeContent initRootContent() {
		SubnodeContent content = new SubnodeContent();
		content.size = 0;
		return content;
	}
	
	public void writeObject(RandomAccessFile raf) throws IOException {
		raf.writeInt(size);
		for(int i = 0; i < MAX_CAPACITY; i++) {
			SerializeUtils.writeString(raf, getSubnodeNames()[i], MAX_NODENAME_LENGTH);
			raf.writeInt(getSubnodeIndex()[i]);
		}
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
