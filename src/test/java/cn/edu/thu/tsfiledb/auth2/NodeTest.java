package cn.edu.thu.tsfiledb.auth2;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import cn.edu.thu.tsfiledb.auth2.exception.UnknownNodeTypeException;
import cn.edu.thu.tsfiledb.auth2.manage.NodeManager;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeHeader;
import cn.edu.thu.tsfiledb.auth2.permTree.PermTreeNode;

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
	}

}
