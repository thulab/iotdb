package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeNode;

public class NodeManager {
	private static String PERMFILE_SUFFIX = ".perm";
	private static String PERMMETA_SUFFIX = ".perm.meta";
	private static String PERM_FOLDER = "perms/";
	
	private static NodeManager instance;

	private int MAX_CACHE_CAPACITY = 1000;
	Map<Pair<Integer, Integer>, PermTreeNode> nodeCache = new HashMap<>();
	LinkedList<Pair<Integer, Integer>> LRUList = new LinkedList<>();

	HashMap<Integer, Integer> initMutexMap = new HashMap<>();
	HashMap<Integer, Integer> accessMutexMap = new HashMap<>();

	private NodeManager() {
		
	}
	
	public static NodeManager getInstance() {
		if(instance == null) {
			instance = new NodeManager();
			instance.init();
		}
		return instance;
	}
	
	private void init() {
		File permFolder = new File(PERM_FOLDER);
		permFolder.mkdirs();
	}
	
	public void initForUser(int uid) throws IOException {
		Integer mutex = initMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			initMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			RandomAccessFile permFileRaf = new RandomAccessFile(uid + PERMFILE_SUFFIX, "rw");
			RandomAccessFile permMetaRaf = new RandomAccessFile(uid + PERMMETA_SUFFIX, "rw");

			PermTreeNode root = PermTreeNode.initRootNode();
			root.writeObject(permFileRaf);
			permMetaRaf.writeInt(1);
			permMetaRaf.close();
			permFileRaf.close();
		}
	}
	
	public void cleanForUser(int uid) {
		Integer mutex = accessMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			accessMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			File permFile = new File(uid + PERMFILE_SUFFIX);
			File permMeta = new File(uid + PERMMETA_SUFFIX);
			permFile.deleteOnExit();
			permMeta.deleteOnExit();
		}
	}

	public PermTreeNode getNode(int uid, int nodeIndex) throws IOException {
		Integer mutex = accessMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			accessMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			Pair<Integer, Integer> index = new Pair<Integer, Integer>(uid, nodeIndex);
			PermTreeNode node = nodeCache.get(index);
			LRUList.remove(index);
			LRUList.addFirst(index);
			if (node != null)
				return node;

			RandomAccessFile permFileRaf = new RandomAccessFile(uid + PERMFILE_SUFFIX, "rw");
			permFileRaf.seek(nodeIndex * PermTreeNode.RECORD_SIZE);
			node = PermTreeNode.readObject(permFileRaf);
			permFileRaf.close();
			nodeCache.put(index, node);
			LRUList.addFirst(index);
			;

			if (nodeCache.size() > MAX_CACHE_CAPACITY) {
				index = LRUList.removeLast();
				nodeCache.remove(index);
			}
			return node;
		}
	}

	synchronized public void putNode(int uid, int nodeIndex, PermTreeNode node) throws IOException {
		Integer mutex = accessMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			accessMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			Pair<Integer, Integer> index = new Pair<Integer, Integer>(uid, nodeIndex);
			nodeCache.put(index, node);
			LRUList.remove(index);
			LRUList.addFirst(index);
			RandomAccessFile permFileRaf = new RandomAccessFile(uid + PERMFILE_SUFFIX, "rw");
			permFileRaf.seek(nodeIndex * PermTreeNode.RECORD_SIZE);
			node.writeObject(permFileRaf);
			permFileRaf.close();
		}
	}

	synchronized public int allocateNode(int uid) throws IOException {
		Integer mutex = accessMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			accessMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			RandomAccessFile permFileRaf = new RandomAccessFile(uid + PERMFILE_SUFFIX, "rw");
			RandomAccessFile permMetaRaf = new RandomAccessFile(uid + PERMMETA_SUFFIX, "rw");

			byte[] emptyBuffer = new byte[PermTreeNode.RECORD_SIZE];
			permFileRaf.seek(permFileRaf.length());
			permFileRaf.write(emptyBuffer);

			int maxUID = permMetaRaf.readInt();
			permMetaRaf.seek(0);
			permMetaRaf.writeInt(maxUID + 1);

			permFileRaf.close();
			permMetaRaf.close();
			return maxUID;
		}
	}
}
