package cn.edu.thu.tsfiledb.auth2.manage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.auth2.exception.InvalidNodeIndexException;
import cn.edu.thu.tsfiledb.auth2.exception.PathAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.UnknownNodeTypeException;
import cn.edu.thu.tsfiledb.auth2.exception.WrongNodetypeException;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeHeader;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeNode;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.utils.PathUtils;

public class NodeManager {
	private static Logger logger = LoggerFactory.getLogger(NodeManager.class);
	private static String PERMFILE_SUFFIX = ".perm";
	private static String PERMMETA_SUFFIX = ".perm.meta";
	private static String PERM_FOLDER = AuthConfig.permFolder;

	private static NodeManager instance;

	private int MAX_CACHE_CAPACITY = 1000;
	Map<Pair<Integer, Integer>, PermTreeNode> nodeCache = new HashMap<>();
	LinkedList<Pair<Integer, Integer>> LRUList = new LinkedList<>();

	HashMap<Integer, Integer> initMutexMap = new HashMap<>();
	HashMap<Integer, Integer> accessMutexMap = new HashMap<>();

	private NodeManager() {

	}

	public static NodeManager getInstance() {
		if (instance == null) {
			instance = new NodeManager();
			instance.init();
		}
		return instance;
	}

	private void init() {
		File permFolder = new File(PERM_FOLDER);
		permFolder.mkdirs();
	}

	/**
	 * create permission file and permission meta file for a user with given uid and
	 * put an empty root node in permission file
	 * 
	 * @param uid
	 * @throws IOException
	 */
	public void initForUser(int uid) throws IOException {
		Integer mutex = initMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			initMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			RandomAccessFile permFileRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMFILE_SUFFIX, "rw");
			RandomAccessFile permMetaRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMMETA_SUFFIX, "rw");

			PermTreeNode root = PermTreeNode.initRootNode();
			root.writeObject(permFileRaf);
			permMetaRaf.writeInt(1);
			permMetaRaf.close();
			permFileRaf.close();
		}
	}

	/**
	 * delete permission file and permission meta file of a user
	 * 
	 * @param uid
	 */
	public void cleanForUser(int uid) {
		Integer mutex = accessMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			accessMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			File permFile = new File(PERM_FOLDER + uid + PERMFILE_SUFFIX);
			File permMeta = new File(PERM_FOLDER + uid + PERMMETA_SUFFIX);
			permFile.delete();
			permMeta.delete();
		}
	}

	/**
	 * read a node from the permission file specified by uid (if node not in cache)
	 * the node will be put in the cache, and if necessary another node will be
	 * replaced
	 * 
	 * @param uid
	 * @param nodeIndex
	 * @return
	 * @throws IOException
	 * @throws UnknownNodeTypeException
	 */
	public PermTreeNode getNode(int uid, int nodeIndex) throws IOException {
		if (nodeIndex < 0) {
			logger.error("invalid node index {}", nodeIndex);
			throw new InvalidNodeIndexException("perm node " + nodeIndex + " is invalid");
		}
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

			RandomAccessFile permFileRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMFILE_SUFFIX, "rw");
			long offset = nodeIndex * PermTreeNode.RECORD_SIZE;
			if (offset > permFileRaf.length()) {
				logger.error("node index {} out of bound {}", nodeIndex,
						permFileRaf.length() / PermTreeNode.RECORD_SIZE);
				throw new InvalidNodeIndexException("perm node " + nodeIndex + " is invalid");
			}
			permFileRaf.seek(offset);
			node = PermTreeNode.readObject(permFileRaf);
			permFileRaf.close();
			nodeCache.put(index, node);
			LRUList.addFirst(index);

			if (nodeCache.size() > MAX_CACHE_CAPACITY) {
				index = LRUList.removeLast();
				nodeCache.remove(index);
			}
			return node;
		}
	}

	/**
	 * Write a node to permission file specified by uid. The position is nodeIndex *
	 * node size.
	 * 
	 * @param uid
	 * @param nodeIndex
	 * @param node
	 * @throws IOException
	 */
	public void putNode(int uid, int nodeIndex, PermTreeNode node) throws IOException {
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
			RandomAccessFile permFileRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMFILE_SUFFIX, "rw");
			permFileRaf.seek(nodeIndex * PermTreeNode.RECORD_SIZE);
			node.writeObject(permFileRaf);
			permFileRaf.close();
		}
	}

	public void putNode(int uid, PermTreeNode node) throws IOException {
		putNode(uid, node.getIndex(), node);
	}

	public int getMaxID(int uid) throws IOException {
		RandomAccessFile permMetaRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMMETA_SUFFIX, "rw");
		int maxUID = permMetaRaf.readInt();
		permMetaRaf.close();
		return maxUID;
	}

	/**
	 * Append a buffer whose size is the size of a node to the permission file by
	 * uid, and increase the max uid in meta file.
	 * 
	 * @param uid
	 * @return
	 * @throws IOException
	 */
	public int allocateID(int uid) throws IOException {
		Integer mutex = accessMutexMap.get(uid);
		if (mutex == null) {
			mutex = new Integer(uid);
			accessMutexMap.put(uid, mutex);
		}
		synchronized (mutex) {
			RandomAccessFile permFileRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMFILE_SUFFIX, "rw");
			RandomAccessFile permMetaRaf = new RandomAccessFile(PERM_FOLDER + uid + PERMMETA_SUFFIX, "rw");

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

	public PermTreeNode allocateNode(int uid, int pid, String nodeName, int nodeType)
			throws UnknownNodeTypeException, IOException {
		PermTreeNode node = new PermTreeNode(nodeName, nodeType, allocateID(uid), pid);
		putNode(uid, node);
		return node;
	}

	public boolean findRole(int uid, PermTreeNode node, int rid) throws IOException {
		PermTreeNode next = node;
		boolean found = false;
		while (!found) {
			found = next.findRole(rid);
			if (next.getRoleExt() == -1)
				break;
			next = getNode(uid, next.getRoleExt());
		}
		return found;
	}

	/**
	 * Collect roles from a given node or its extensions.
	 * 
	 * @param uid
	 * @param node
	 * @return
	 * @throws IOException
	 */
	public Set<Integer> findRoles(int uid, PermTreeNode node) throws IOException {
		Set<Integer> roleSet = new HashSet<>();
		PermTreeNode next = node;
		while (true) {
			roleSet.addAll(next.findRoles());
			if (next.getRoleExt() == -1)
				break;
			next = getNode(uid, next.getRoleExt());
		}
		return roleSet;
	}

	/**
	 * search a child in a node and its sub-nodes.
	 * 
	 * @param uid
	 * @param node
	 * @param childName
	 * @return the index of the child, -1 when not found
	 * @throws WrongNodetypeException
	 * @throws IOException
	 */
	public int findChild(int uid, PermTreeNode node, String childName) throws WrongNodetypeException, IOException {
		int childIndex = node.findChild(childName);
		PermTreeNode next = node;
		while (childIndex == -1 && next.getSubnodeExt() != -1) {
			next = getNode(uid, next.getSubnodeExt());
			childIndex = next.findChild(childName);
		}
		return childIndex;
	}

	/**
	 * Try to add a child to given node or its extension. If all nodes are full, a
	 * new extension will be created.
	 * 
	 * @param uid
	 * @param node
	 * @param childName
	 * @param cid
	 * @return true if the child is successfully added, false if the child already
	 *         exist;
	 * @throws WrongNodetypeException
	 * @throws IOException
	 * @throws UnknownNodeTypeException
	 * @throws PathAlreadyExistException
	 */
	public boolean addChild(int uid, PermTreeNode node, String childName, int cid)
			throws WrongNodetypeException, IOException, UnknownNodeTypeException, PathAlreadyExistException {
		if (findChild(uid, node, childName) != -1) {
			logger.error("{} already has child {}", node.getName(), childName);
			throw new PathAlreadyExistException(childName + " in " + node.getName() + " already exists");
		}
		PermTreeNode curnode = node;
		boolean added = curnode.addChild(childName, cid);
		int curIndex = curnode.getIndex();
		int nextIndex = curnode.getSubnodeExt();
		// try adding in existing nodes
		while (!added && nextIndex != -1) {
			curnode = getNode(uid, nextIndex);
			added = curnode.addChild(childName, cid);
			curIndex = nextIndex;
			nextIndex = curnode.getSubnodeExt();
		}
		// if all nodes are full, allocate a new node as extension
		if (!added) {
			nextIndex = allocateID(uid);
			PermTreeNode newNode = new PermTreeNode(curnode.getName(), PermTreeHeader.SUBNODE_EXTENSION, nextIndex,
					curIndex);
			newNode.addChild(childName, cid);
			curnode.setSubnodeExt(nextIndex);
			putNode(uid, curnode.getIndex(), curnode);
			putNode(uid, nextIndex, newNode);
		} else {
			putNode(uid, curnode.getIndex(), curnode);
		}
		return true;
	}

	public boolean addChild(int uid, PermTreeNode node, PermTreeNode child)
			throws WrongNodetypeException, IOException, UnknownNodeTypeException, PathAlreadyExistException {
		return addChild(uid, node, child.getName(), child.getIndex());
	}

	/**
	 * Delete a child in a node and its extensions
	 * 
	 * @param uid
	 * @param node
	 * @param childName
	 * @return true if the child is found and deleted, false if no such child is
	 *         found
	 * @throws WrongNodetypeException
	 * @throws IOException
	 */
	public boolean deleteChild(int uid, PermTreeNode node, String childName)
			throws WrongNodetypeException, IOException {
		boolean deleted = node.deleteChild(childName);
		PermTreeNode next = node;
		while (!deleted && next.getSubnodeExt() != -1) {
			next = getNode(uid, next.getSubnodeExt());
			deleted = next.deleteChild(childName);
		}
		if (deleted) {
			putNode(uid, next.getIndex(), next);
		}
		return deleted;
	}

	/**
	 * Get the lead node corresponding to "path" of user "uid". Internal nodes are
	 * created when not existing, so this method can be used as a mkdir.
	 * 
	 * @param uid
	 * @param path
	 * @return node of the last level of the path
	 * @throws PathErrorException
	 * @throws IOException
	 * @throws WrongNodetypeException
	 * @throws UnknownNodeTypeException
	 * @throws PathAlreadyExistException
	 */
	public PermTreeNode getLeaf(int uid, String path)
			throws PathErrorException, IOException, WrongNodetypeException, PathAlreadyExistException {
		String[] pathLevels = PathUtils.getPathLevels(path);
		PermTreeNode next = getNode(uid, 0);
		for (int i = 1; i < pathLevels.length; i++) {
			int childIndex = findChild(uid, next, pathLevels[i]);
			// when a child does not exist, create a new node
			if (childIndex == -1) {
				childIndex = allocateID(uid);
				PermTreeNode child = new PermTreeNode(pathLevels[i], PermTreeHeader.NORMAL_NODE, childIndex,
						next.getIndex());
				addChild(uid, next, child);
				putNode(uid, child);
			}
			next = getNode(uid, childIndex);
		}
		return next;
	}

	/**
	 * add a role "rid" to "node" or its extensions
	 * 
	 * @param uid
	 * @param node
	 * @param rid
	 * @return true if the role is added, false if the role already exist
	 * @throws IOException
	 * @throws UnknownNodeTypeException
	 */
	public boolean addRole(int uid, PermTreeNode node, int rid) throws IOException, UnknownNodeTypeException {
		if (findRole(uid, node, rid)) {
			logger.error("role {} already in {}", rid, node.getName());
			return false;
		}
		PermTreeNode curnode = node;
		boolean added = curnode.addRole(rid);
		int curIndex = curnode.getIndex();
		int nextIndex = curnode.getRoleExt();
		// try adding in existing nodes
		while (!added && nextIndex != -1) {
			curnode = getNode(uid, nextIndex);
			added = curnode.addRole(rid);
			curIndex = nextIndex;
			nextIndex = curnode.getRoleExt();
		}
		// if all nodes are full, allocate a new node as extension
		if (!added) {
			nextIndex = allocateID(uid);
			PermTreeNode newNode = new PermTreeNode(curnode.getName(), PermTreeHeader.ROLE_EXTENSION, nextIndex,
					curIndex);
			newNode.addRole(rid);
			curnode.setRoleExt(nextIndex);
			putNode(uid, curnode.getIndex(), curnode);
			putNode(uid, nextIndex, newNode);
		} else {
			putNode(uid, curnode.getIndex(), curnode);
		}
		return true;
	}

	/**
	 * delete a role <rid> of <node>
	 * 
	 * @param uid
	 * @param node
	 * @param rid
	 * @return true if the role is found and deleted, false if the role not exist
	 * @throws IOException
	 */
	public boolean deleteRole(int uid, PermTreeNode node, int rid) throws IOException {
		boolean deleted = node.deleteRole(rid);
		PermTreeNode next = node;
		while (!deleted && next.getRoleExt() != -1) {
			next = getNode(uid, next.getRoleExt());
			deleted = next.deleteRole(rid);
		}
		if (deleted) {
			putNode(uid, next.getIndex(), next);
		}
		return deleted;
	}
}
