package cn.edu.thu.tsfiledb.auth2.permTree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.PathAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.RoleAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.UnknownNodeTypeException;
import cn.edu.thu.tsfiledb.auth2.exception.WrongNodetypeException;

public class PermTreeNode {
	private static Logger logger = LoggerFactory.getLogger(PermTreeNode.class);
	public static int RECORD_SIZE = PermTreeHeader.RECORD_SIZE + PermTreeContent.RECORD_SIZE;

	private PermTreeHeader header;
	private PermTreeContent content;

	public PermTreeNode() {
	}

	/**
	 * @param nodeName
	 * @param nodeType
	 * @param ID
	 *            ID of this node
	 * @param PID
	 *            ID of parent node
	 * @throws UnknownNodeTypeException
	 */
	public PermTreeNode(String nodeName, int nodeType, int ID, int PID) throws UnknownNodeTypeException {
		this.header = new PermTreeHeader();
		this.header.setNodeName(nodeName);
		this.header.setNodeType(nodeType);
		this.header.setCurrentIndex(ID);
		this.header.setParentIndex(PID);
		switch (nodeType) {
		case PermTreeHeader.NORMAL_NODE:
		case PermTreeHeader.SUBNODE_EXTENSION:
			this.content = new SubnodeContent();
			break;
		case PermTreeHeader.ROLE_EXTENSION:
			this.content = new RoleContent();
			break;
		default:
			logger.error("unrecognized node type {} when init node for {}", nodeType, nodeName);
			throw new UnknownNodeTypeException();
		}
	}

	public static PermTreeNode readObject(RandomAccessFile raf) throws IOException, UnknownNodeTypeException {
		PermTreeNode node = new PermTreeNode();
		node.header = PermTreeHeader.readObject(raf);
		int nodeType = node.header.getNodeType();
		switch (nodeType) {
		case PermTreeHeader.NORMAL_NODE:
		case PermTreeHeader.SUBNODE_EXTENSION:
			node.content = SubnodeContent.readObject(raf);
			break;
		case PermTreeHeader.ROLE_EXTENSION:
			node.content = RoleContent.readObject(raf);
			break;
		default:
			logger.error("unrecognized node type {} when init node for {}", nodeType, node.header.getNodeName());
			throw new UnknownNodeTypeException();
		}
		
		return node;
	}

	public void writeObject(RandomAccessFile raf) throws IOException {
		header.writeObject(raf);
		content.writeObject(raf);
	}

	public static PermTreeNode initRootNode() {
		PermTreeNode node = new PermTreeNode();
		node.header = PermTreeHeader.initRootHeader();
		node.content = SubnodeContent.initRootContent();
		return node;
	}

	/** add a child with given info in THIS node
	 * caller should check exist
	 * @param childName
	 * @param cid
	 * @return true if success, false if THIS node is full
	 * @throws WrongNodetypeException
	 * @throws PathAlreadyExistException
	 */
	public boolean addChild(String childName, int cid) throws WrongNodetypeException, PathAlreadyExistException {
		// caller check
		/*if (findChild(childName) != -1){
			logger.error(childName + " already exists");
			throw new PathAlreadyExistException(childName);
		}*/
		if (header.getNodeType() != PermTreeHeader.NORMAL_NODE
				&& header.getNodeType() != PermTreeHeader.SUBNODE_EXTENSION) {
			logger.error("add child to a role extension");
			throw new WrongNodetypeException(String.valueOf(header.getNodeType()));
		}
		SubnodeContent subnodeContent = (SubnodeContent) content;
		return subnodeContent.addChild(childName, cid);
	}

	public boolean deleteChild(String childName) throws WrongNodetypeException {
		if (header.getNodeType() != PermTreeHeader.NORMAL_NODE
				&& header.getNodeType() != PermTreeHeader.SUBNODE_EXTENSION) {
			logger.error("add child to a role extension");
			throw new WrongNodetypeException(String.valueOf(header.getNodeType()));
		}
		SubnodeContent subnodeContent = (SubnodeContent) content;
		return subnodeContent.deleteChild(childName);
	}

	/**
	 * Try to find a child by name within this node.
	 * 
	 * @param childName
	 * @return the index of this child, -1 when no child found
	 * @throws WrongNodetypeException
	 */
	public int findChild(String childName) throws WrongNodetypeException {
		if (!(content instanceof SubnodeContent)) {
			logger.error("trying to find child {} of a node with no child", childName);
			throw new WrongNodetypeException();
		}
		SubnodeContent nodeContent = (SubnodeContent) content;
		for (int i = 0; i < nodeContent.getSize(); i++) {
			if (nodeContent.getSubnodeNames()[i].equals(childName))
				return nodeContent.getSubnodeIndex()[i];
		}
		return -1;
	}

	public boolean findRole(int rid) {
		for (int i = 0; i < header.getRoleNum(); i++) {
			if (header.getRoles()[i] == rid)
				return true;
		}
		if (content instanceof RoleContent) {
			RoleContent roleContent = (RoleContent) content;
			for (int i = 0; i < roleContent.getRoleNum(); i++) {
				if (roleContent.getRoles()[i] == rid)
					return true;
			}
		}
		return false;
	}
	
	/**
	 * collect the roles in the header, if this node is a role-extension also
	 * collect the roles in the content
	 * 
	 * @return
	 */
	public Set<Integer> findRoles() {
		Set<Integer> roles = new HashSet<>();
		for (int i = 0; i < header.getRoleNum(); i++) {
			if (header.getRoles()[i] != -1)
				roles.add(header.getRoles()[i]);
		}
		if (content instanceof RoleContent) {
			RoleContent roleContent = (RoleContent) content;
			for (int i = 0; i < roleContent.getRoleNum(); i++) {
				if (header.getRoles()[i] != -1)
					roles.add(roleContent.getRoles()[i]);
			}
		}
		return roles;
	}

	/**
	 * delete a given role by ID within this node, the method is to set the roleID
	 * in the entry to -1
	 * 
	 * @param roleID
	 * @return if the role is found and deleted
	 */
	public boolean deleteRole(int roleID) {
		boolean deleted = false;
		for (int i = 0; i < header.getRoleNum(); i++) {
			if (header.getRoles()[i] == roleID) {
				header.getRoles()[i] = -1;
				deleted = true;
				if (i == header.getRoleNum() - 1) {
					header.setRoleNum(header.getRoleNum() - 1);
				} else {
					header.setEmptyRoleNum(header.getEmptyRoleNum() + 1);
				}
			}
		}
		if (content instanceof RoleContent && !deleted) {
			RoleContent roleContent = (RoleContent) content;
			deleted = roleContent.deleteRole(roleID);
		}
		return deleted;
	}

	/**
	 * try to add a role by ID with in this node
	 * caller should check exist
	 * 
	 * @param roleID
	 * @return true if the role is added, false when the node is full depending on
	 *         node manager to issue another attempt
	 * @throws RoleAlreadyExistException
	 */
	public boolean addRole(int roleID) throws RoleAlreadyExistException {
		boolean added = false;
		// caller check
		/*if (findRole(roleID)) {
			throw new RoleAlreadyExistException("the role has already been granted to the node");
		}*/
		// try to add in header
		if (header.getEmptyRoleNum() > 0) {
			for (int i = 0; i < header.getRoleNum(); i++) {
				if (header.getRoles()[i] == -1) {
					header.getRoles()[i] = roleID;
					header.setEmptyRoleNum(header.getEmptyRoleNum() - 1);
					added = true;
				}
			}
		} else if (header.getRoleNum() < PermTreeHeader.MAX_ROLE_NUM) {
			header.getRoles()[header.getRoleNum()] = roleID;
			added = true;
			header.setRoleNum(header.getRoleNum() + 1);
		}
		// try to add in content
		if (content instanceof RoleContent) {
			RoleContent rContent = (RoleContent) content;
			added = rContent.addRole(roleID);
		}
		return added;
	}

	public void setParent(int index) {
		header.setParentIndex(index);
	}

	public int getParent() {
		return header.getParentIndex();
	}

	public void setSubnodeExt(int index) {
		header.setSubnodeExtIndex(index);
	}

	public int getSubnodeExt() {
		return header.getSubnodeExtIndex();
	}

	public void setRoleExt(int index) {
		header.setRoleExtIndex(index);
	}

	public int getRoleExt() {
		return header.getRoleExtIndex();
	}

	public int getIndex() {
		return header.getCurrentIndex();
	}

	public String getName() {
		return header.getNodeName();
	}

	public PermTreeHeader getHeader() {
		return header;
	}

	public void setHeader(PermTreeHeader header) {
		this.header = header;
	}

	public PermTreeContent getContent() {
		return content;
	}

	public void setContent(PermTreeContent content) {
		this.content = content;
	}
}
