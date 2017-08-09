package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.exception.AuthException;
import cn.edu.thu.tsfiledb.auth2.exception.PathAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.RoleAlreadyExistException;
import cn.edu.thu.tsfiledb.auth2.exception.UnknownNodeTypeException;
import cn.edu.thu.tsfiledb.auth2.exception.WrongNodetypeException;
import cn.edu.thu.tsfiledb.auth2.manage.NodeManager;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeHeader;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeNode;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class NodeTest {

	@Test
	public void createTest() throws IOException, UnknownNodeTypeException {
		NodeManager nodeManager = NodeManager.getInstance();
		int uid = 1;
		
		nodeManager.initForUser(uid);
		PermTreeNode root = nodeManager.getNode(uid, 0);
		assertTrue(root.getHeader().getNodeName().equals("root"));
		
		int nextPage = nodeManager.allocateID(uid);
		PermTreeNode roleExt = new PermTreeNode("roleExt", PermTreeHeader.ROLE_EXTENSION, nextPage, 0);
		nodeManager.putNode(uid, nextPage, roleExt);
		nextPage = nodeManager.allocateID(uid);
		PermTreeNode childExt = new PermTreeNode("childExt", PermTreeHeader.SUBNODE_EXTENSION, nextPage, 0);
		nodeManager.putNode(uid, nextPage, childExt);
		root.setRoleExt(roleExt.getIndex());
		root.setSubnodeExt(childExt.getIndex());
		
		PermTreeNode node = nodeManager.getNode(uid, root.getRoleExt());
		assertTrue(node.getName().equals("roleExt"));
		node = nodeManager.getNode(uid, root.getSubnodeExt());
		assertTrue(node.getName().equals("childExt"));
		nodeManager.cleanForUser(uid);
	}
	
	@Test
	public void addChildTest() throws IOException, UnknownNodeTypeException, AuthException {
		// all 1000 children add into one node
		NodeManager nodeManager = NodeManager.getInstance();
		int uid = 2;
		
		nodeManager.cleanForUser(uid);
		nodeManager.initForUser(uid);
		PermTreeNode root = nodeManager.getNode(uid, 0);
		for(int i = 0; i < 1000; i++) {
			PermTreeNode child = nodeManager.allocateNode(uid, 0, "node" + i, PermTreeHeader.NORMAL_NODE);
			nodeManager.addChild(uid, root, child);
		}
		root = nodeManager.getNode(uid, 0);
		assertTrue(root.getSubnodeExt() != -1);
		String nodeName = "node" + 450;
		int nodeIndex = nodeManager.findChild(uid, root, nodeName);
		assertNotEquals(nodeIndex, -1);
		PermTreeNode node = nodeManager.getNode(uid, nodeIndex);
		assertEquals(node.getName(), nodeName);
		try {
			nodeManager.addChild(uid, root, node);
		} catch (Exception e) {
			assertTrue(e instanceof PathAlreadyExistException);
		}
		nodeManager.cleanForUser(uid);
	}
	
	@Test
	public void addChildTest2() throws IOException, WrongNodetypeException, PathAlreadyExistException {
		// add 1000 children in 10 nodes
		NodeManager nodeManager = NodeManager.getInstance();
		int uid = 3;
		
		nodeManager.cleanForUser(uid);
		nodeManager.initForUser(uid);
		PermTreeNode root = nodeManager.getNode(uid, 0);
		for(int i = 0; i < 10; i ++) {
			PermTreeNode child = nodeManager.allocateNode(uid, 0, "node" + i, PermTreeHeader.NORMAL_NODE);
			assertTrue(nodeManager.addChild(uid, root, child));
		}
		for(int i = 0; i < 1000; i++) {
			PermTreeNode child = nodeManager.allocateNode(uid, 0, "subnode" + i, PermTreeHeader.NORMAL_NODE);
			root = nodeManager.getNode(uid, (i%10 + 1));
			assertTrue(nodeManager.addChild(uid, root, child));
		}
		nodeManager.cleanForUser(uid);
	}

	@Test
	public void deleteChildTest() throws IOException, WrongNodetypeException, PathAlreadyExistException {
		NodeManager nodeManager = NodeManager.getInstance();
		int uid = 4;
		
		nodeManager.cleanForUser(uid);
		nodeManager.initForUser(uid);
		PermTreeNode root = nodeManager.getNode(uid, 0);
		PermTreeNode child = nodeManager.allocateNode(uid, root.getIndex(), "child", PermTreeHeader.NORMAL_NODE);
		nodeManager.addChild(uid, root, child);
		
		int childIndex = nodeManager.findChild(uid, root, child.getName());
		assertNotEquals(childIndex, -1);
		nodeManager.deleteChild(uid, root, child.getName());
		childIndex = nodeManager.findChild(uid, root, child.getName());
		assertEquals(childIndex, -1);
		
		assertFalse(nodeManager.deleteChild(uid, root, child.getName()));
	}
	
	@Test 
	public void roleTest() throws IOException, RoleAlreadyExistException {
		NodeManager nodeManager = NodeManager.getInstance();
		int uid = 4;
		// add roles
		nodeManager.cleanForUser(uid);
		nodeManager.initForUser(uid);
		PermTreeNode root = nodeManager.getNode(uid, 0);
		for(int i = 0; i < 1000; i ++) {
			assertTrue(nodeManager.addRole(uid, root, i));
		}
		root = nodeManager.getNode(uid, 0);
		assertNotEquals(root.getRoleExt(), -1);
		assertTrue(nodeManager.findRole(uid, root, 450));
		assertFalse(nodeManager.findRole(uid, root, 4500));
		Set<Integer> roles = nodeManager.findRoles(uid, root);
		assertEquals(roles.size(), 1000);
		
		try {
			nodeManager.addRole(uid, root, 450);
		} catch (Exception e) {
			assertTrue(e instanceof RoleAlreadyExistException);
		}
		
		// delete role
		assertTrue(nodeManager.deleteRole(uid, root, 450));
		assertTrue(nodeManager.addRole(uid, root, 450));
		nodeManager.cleanForUser(uid);
	}
	
	@Test
	public void getLeafTest() throws IOException, PathErrorException, AuthException {
		// randomly add 1000 paths with depth of 10
		NodeManager nodeManager = NodeManager.getInstance();
		int uid = 5;
		
		nodeManager.cleanForUser(uid);
		nodeManager.initForUser(uid);
		
		try {
			nodeManager.getLeaf(uid, "not root");
		} catch (Exception e) {
			assertTrue(e instanceof PathErrorException);
		}
		
		String rootStr = "root";
		for(int i = 0; i < 1000; i++) {
			String path = rootStr;
			for(int j = 0; j < 10; j++) {
				path += "." + i;
			}
			nodeManager.getLeaf(uid, path);
		}
		PermTreeNode root = nodeManager.getNode(uid, 0);
		for(int i = 0; i < 10; i++) {
			int childIndex = nodeManager.findChild(uid, root, "666");
			assertNotEquals(childIndex, -1);
			root = nodeManager.getNode(uid, childIndex);
		}
	}
}
