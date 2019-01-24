/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

/**
 * The hierarchical struct of the Metadata Tree is implemented in this class.
 */
public class MTree implements Serializable {

  private static final long serialVersionUID = -4200394435237291964L;
  private final String space = "    ";
  private final String separator = "\\.";
  private MNode root;

  public MTree(String rootName) {
    this.root = new MNode(rootName, null, false);
  }

  public MTree(MNode root) {
    this.root = root;
  }

  /**
   * function for adding timeseries.It should check whether seriesPath exists.
   */
  public void addTimeseriesPath(String timeseriesPath, String dataType, String encoding,
      String[] args)
      throws PathErrorException {
    String[] nodeNames = timeseriesPath.trim().split(separator);
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not right.", timeseriesPath));
    }
    int i = 1;
    MNode cur = root;
    String levelPath = null;
    while (i < nodeNames.length) {
      String nodeName = nodeNames[i];
      if (i == nodeNames.length - 1) {
        cur.setDataFileName(levelPath);
        break;
      }
      if (cur.isStorageLevel()) {
        levelPath = cur.getDataFileName();
      }
      if (!cur.hasChild(nodeName)) {
        if (cur.isLeaf()) {
          throw new PathErrorException(
              String.format("The Node [%s] is left node, the timeseries %s can't be created",
                  cur.getName(), timeseriesPath));
        }
        cur.addChild(nodeName, new MNode(nodeName, cur, false));
      }
      cur.setDataFileName(levelPath);
      cur = cur.getChild(nodeName);
      if (levelPath == null) {
        levelPath = cur.getDataFileName();
      }
      i++;
    }
    TSDataType dt = TSDataType.valueOf(dataType);
    TSEncoding ed = TSEncoding.valueOf(encoding);
    MNode leaf = new MNode(nodeNames[nodeNames.length - 1], cur, dt, ed);
    if (args.length > 0) {
      for (int k = 0; k < args.length; k++) {
        String[] arg = args[k].split("=");
        leaf.getSchema().putKeyValueToArgs(arg[0], arg[1]);
      }
    }
    levelPath = cur.getDataFileName();
    leaf.setDataFileName(levelPath);
    if (cur.isLeaf()) {
      throw new PathErrorException(
          String.format("The Node [%s] is left node, the timeseries %s can't be created",
              cur.getName(), timeseriesPath));
    }
    cur.addChild(nodeNames[nodeNames.length - 1], leaf);
  }

  /**
   * function for checking whether the given path exists.
   *
   * @param path -seriesPath not necessarily the whole seriesPath (possibly a prefix of a sequence)
   */
  public boolean isPathExist(String path) {
    String[] nodeNames = path.trim().split(separator);
    MNode cur = root;
    int i = 0;
    while (i < nodeNames.length - 1) {
      String nodeName = nodeNames[i];
      if (cur.getName().equals(nodeName)) {
        i++;
        nodeName = nodeNames[i];
        if (cur.hasChild(nodeName)) {
          cur = cur.getChild(nodeName);
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    if (cur.getName().equals(nodeNames[i])) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * function for checking whether the given path exists under the given mnode.
   */
  public boolean isPathExist(MNode node, String path) {
    String[] nodeNames = path.trim().split(separator);
    if (nodeNames.length < 1) {
      return true;
    }
    if (!node.hasChild(nodeNames[0])) {
      return false;
    }
    MNode cur = node.getChild(nodeNames[0]);

    int i = 0;
    while (i < nodeNames.length - 1) {
      String nodeName = nodeNames[i];
      if (cur.getName().equals(nodeName)) {
        i++;
        nodeName = nodeNames[i];
        if (cur.hasChild(nodeName)) {
          cur = cur.getChild(nodeName);
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
    if (cur.getName().equals(nodeNames[i])) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * make sure check seriesPath before setting storage group.
   */
  public void setStorageGroup(String path) throws PathErrorException {
    String[] nodeNames = path.split(separator);
    MNode cur = root;
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new PathErrorException(
          String.format("The storage group can't be set to the %s node", path));
    }
    int i = 1;
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        // add one child node
        cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
      } else if (temp.isStorageLevel()) {
        // before set storage group should check the seriesPath exist or not
        // throw exception
        throw new PathErrorException(
            String.format("The prefix of %s has been set to the storage group.", path));
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    MNode temp = cur.getChild(nodeNames[i]);
    if (temp == null) {
      cur.addChild(nodeNames[i], new MNode(nodeNames[i], cur, false));
    } else {
      throw new PathErrorException(
          String.format("The seriesPath of %s already exist, it can't be set to the storage group",
              path));
    }
    cur = cur.getChild(nodeNames[i]);
    cur.setStorageLevel(true);
    setDataFileName(path, cur);
  }

  /**
   * Check whether set file seriesPath for this node or not. If not, throw an exception
   */
  private void checkStorageGroup(MNode node) throws PathErrorException {
    if (node.getDataFileName() != null) {
      throw new PathErrorException(
          String.format("The storage group %s has been set", node.getDataFileName()));
    }
    if (node.getChildren() == null) {
      return;
    }
    for (MNode child : node.getChildren().values()) {
      checkStorageGroup(child);
    }
  }

  private void setDataFileName(String path, MNode node) {
    node.setDataFileName(path);
    if (node.getChildren() == null) {
      return;
    }
    for (MNode child : node.getChildren().values()) {
      setDataFileName(path, child);
    }
  }

  /**
   * Delete one seriesPath from current Metadata Tree.
   *
   * @param path Format: root.node.(node)* Notice: Path must be a complete Path from root to leaf
   * node.
   */
  public String deletePath(String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException("Timeseries %s is not correct." + path);
    }

    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:"
                + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
    }

    // if the storage group node is deleted, the dataFileName should be
    // return
    String dataFileName = null;
    if (cur.isStorageLevel()) {
      dataFileName = cur.getDataFileName();
    }
    cur.getParent().deleteChild(cur.getName());
    cur = cur.getParent();
    while (cur != null && !cur.getName().equals("root") && cur.getChildren().size() == 0) {
      if (cur.isStorageLevel()) {
        dataFileName = cur.getDataFileName();
        return dataFileName;
      }
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }

    return dataFileName;
  }

  /**
   * Check whether the seriesPath given exists.
   */
  public boolean hasPath(String path) {
    String[] nodes = path.split(separator);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      return false;
    }
    return hasPath(getRoot(), nodes, 1);
  }

  private boolean hasPath(MNode node, String[] nodes, int idx) {
    if (idx >= nodes.length) {
      return true;
    }
    if (nodes[idx].equals("*")) {
      boolean res = false;
      for (MNode child : node.getChildren().values()) {
        res |= hasPath(child, nodes, idx + 1);
      }
      return res;
    } else {
      if (node.hasChild(nodes[idx])) {
        return hasPath(node.getChild(nodes[idx]), nodes, idx + 1);
      }
      return false;
    }
  }

  /**
   * Get ColumnSchema for given seriesPath. Notice: Path must be a complete Path from root to leaf
   * node.
   */
  public ColumnSchema getSchemaForOnePath(String path) throws PathErrorException {
    MNode leaf = getLeafByPath(path);
    return leaf.getSchema();
  }

  public ColumnSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {
    MNode leaf = getLeafByPath(node, path);
    return leaf.getSchema();
  }

  public ColumnSchema getSchemaForOnePathWithCheck(MNode node, String path)
      throws PathErrorException {
    MNode leaf = getLeafByPathWithCheck(node, path);
    return leaf.getSchema();
  }

  public ColumnSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {
    MNode leaf = getLeafByPathWithCheck(path);
    return leaf.getSchema();
  }

  private MNode getLeafByPath(String path) throws PathErrorException {
    checkPath(path);
    String[] node = path.split(separator);
    MNode cur = getRoot();
    for (int i = 1; i < node.length; i++) {
      cur = cur.getChild(node[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format("Timeseries %s is not the leaf node", path));
    }
    return cur;
  }

  private MNode getLeafByPath(MNode node, String path) throws PathErrorException {
    checkPath(node, path);
    String[] nodes = path.split(separator);
    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format("Timeseries %s is not the leaf node", path));
    }
    return cur;
  }

  private MNode getLeafByPathWithCheck(MNode node, String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length < 1 || !node.hasChild(nodes[0])) {
      throw new PathErrorException(String.format("Timeseries %s is not correct", path));
    }

    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:"
                + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format("Timeseries %s is not the leaf node", path));
    }
    return cur;
  }

  private MNode getLeafByPathWithCheck(String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not correct", path));
    }

    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:"
                + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
    }
    if (!cur.isLeaf()) {
      throw new PathErrorException(String.format("Timeseries %s is not the leaf node", path));
    }
    return cur;
  }

  /**
   * function for getting node by path.
   */
  public MNode getNodeByPath(String path) throws PathErrorException {
    checkPath(path);
    String[] node = path.split(separator);
    MNode cur = getRoot();
    for (int i = 1; i < node.length; i++) {
      cur = cur.getChild(node[i]);
    }
    return cur;
  }

  /**
   * function for getting node by path with file level check.
   */
  public MNode getNodeByPathWithFileLevelCheck(String path) throws PathErrorException {
    boolean fileLevelChecked = false;
    String[] nodes = path.split(separator);
    if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not correct", path));
    }

    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:"
                + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
      if (cur.isStorageLevel()) {
        fileLevelChecked = true;
      }
    }
    if (!fileLevelChecked) {
      throw new PathErrorException("FileLevel is not set for current seriesPath:" + path);
    }
    return cur;
  }

  /**
   * Extract the deviceType from given seriesPath.
   *
   * @return String represents the deviceId
   */
  public String getDeviceTypeByPath(String path) throws PathErrorException {
    checkPath(path);
    String[] nodes = path.split(separator);
    if (nodes.length < 2) {
      throw new PathErrorException(
          String.format("Timeseries %s must have two or more nodes", path));
    }
    return nodes[0] + "." + nodes[1];
  }

  /**
   * Check whether a seriesPath is available.
   *
   * @return last node in given seriesPath if current seriesPath is available
   */
  private MNode checkPath(String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length < 2 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not correct", path));
    }
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:"
                + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
    }
    return cur;
  }

  private void checkPath(MNode node, String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length < 1) {
      return;
    }
    MNode cur = node;
    for (int i = 0; i < nodes.length; i++) {
      if (!cur.hasChild(nodes[i])) {
        throw new PathErrorException(
            "Timeseries is not correct. Node[" + cur.getName() + "] doesn't have child named:"
                + nodes[i]);
      }
      cur = cur.getChild(nodes[i]);
    }
  }

  /**
   * Get the storage group seriesPath from the seriesPath.
   *
   * @return String storage group seriesPath
   */
  public String getFileNameByPath(String path) throws PathErrorException {

    String[] nodes = path.split(separator);
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        throw new PathErrorException(
            String.format("The prefix of the seriesPath %s is not one storage group seriesPath",
                path));
      } else if (cur.isStorageLevel()) {
        return cur.getDataFileName();
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    if (cur.isStorageLevel()) {
      return cur.getDataFileName();
    }
    throw new PathErrorException(
        String.format("The prefix of the seriesPath %s is not one storage group seriesPath", path));
  }

  /**
   * function for getting file name by path.
   */
  public String getFileNameByPath(MNode node, String path) throws PathErrorException {

    String[] nodes = path.split(separator);
    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        throw new PathErrorException(
            String.format("The prefix of the seriesPath %s is not one storage group seriesPath",
                path));
      } else if (cur.isStorageLevel()) {
        return cur.getDataFileName();
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    if (cur.isStorageLevel()) {
      return cur.getDataFileName();
    }
    throw new PathErrorException(
        String.format("The prefix of the seriesPath %s is not one storage group seriesPath", path));
  }

  /**
   * function for getting file name by path with check.
   */
  public String getFileNameByPathWithCheck(MNode node, String path) throws PathErrorException {

    String[] nodes = path.split(separator);
    if (nodes.length < 1 || !node.hasChild(nodes[0])) {
      throw new PathErrorException(
          String
              .format("The prefix of the seriesPath %s is not one storage group seriesPath",
                  path));
    }

    MNode cur = node.getChild(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      if (cur == null) {
        throw new PathErrorException(
            String.format("The prefix of the seriesPath %s is not one storage group seriesPath",
                path));
      } else if (cur.isStorageLevel()) {
        return cur.getDataFileName();
      } else {
        if (!cur.hasChild(nodes[i])) {
          throw new PathErrorException(
              String.format("The prefix of the seriesPath %s is not one storage group seriesPath",
                  path));
        }
        cur = cur.getChild(nodes[i]);
      }
    }
    if (cur.isStorageLevel()) {
      return cur.getDataFileName();
    }
    throw new PathErrorException(
        String.format("The prefix of the seriesPath %s is not one storage group seriesPath", path));
  }

  /**
   * Check the prefix of this seriesPath is storage group seriesPath.
   *
   * @return true the prefix of this seriesPath is storage group seriesPath false the prefix of this
   * seriesPath is not storage group seriesPath
   */
  public boolean checkFileNameByPath(String path) {

    String[] nodes = path.split(separator);
    MNode cur = getRoot();
    for (int i = 1; i <= nodes.length; i++) {
      if (cur == null) {
        return false;
      } else if (cur.isStorageLevel()) {
        return true;
      } else {
        cur = cur.getChild(nodes[i]);
      }
    }
    return false;
  }

  /**
   * Get all paths for given seriesPath regular expression Regular expression in this method is
   * formed by the amalgamation of seriesPath and the character '*'.
   *
   * @return A HashMap whose Keys are separated by the storage file name.
   */
  public HashMap<String, ArrayList<String>> getAllPath(String pathReg) throws PathErrorException {
    HashMap<String, ArrayList<String>> paths = new HashMap<>();
    String[] nodes = pathReg.split(separator);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not correct", pathReg));
    }
    findPath(getRoot(), nodes, 1, "", paths);
    return paths;
  }

  /**
   * function for getting all timeseries paths under the given seriesPath.
   */
  public List<List<String>> getShowTimeseriesPath(String pathReg) throws PathErrorException {
    List<List<String>> res = new ArrayList<>();
    String[] nodes = pathReg.split(separator);
    if (nodes.length == 0 || !nodes[0].equals(getRoot().getName())) {
      throw new PathErrorException(String.format("Timeseries %s is not correct", pathReg));
    }
    findPath(getRoot(), nodes, 1, "", res);
    return res;
  }

  /**
   * function for getting leaf node path in the next level of the given path.
   *
   * @return All leaf nodes' seriesPath(s) of given seriesPath.
   */
  public List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
    List<String> ret = new ArrayList<>();
    MNode cur = checkPath(path);
    for (MNode child : cur.getChildren().values()) {
      if (child.isLeaf()) {
        ret.add(new StringBuilder(path).append(".").append(child.getName()).toString());
      }
    }
    return ret;
  }

  /**
   * function for getting all paths in list.
   */
  public ArrayList<String> getAllPathInList(String path) throws PathErrorException {
    ArrayList<String> res = new ArrayList<>();
    HashMap<String, ArrayList<String>> mapRet = getAllPath(path);
    for (ArrayList<String> value : mapRet.values()) {
      res.addAll(value);
    }
    return res;
  }

  /**
   * Calculate the count of storage-level nodes included in given seriesPath.
   *
   * @return The total count of storage-level nodes.
   */
  public int getFileCountForOneType(String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length != 2 || !nodes[0].equals(getRoot().getName()) || !getRoot()
        .hasChild(nodes[1])) {
      throw new PathErrorException(
          "Timeseries must be " + getRoot().getName()
              + ". X (X is one of the nodes of root's children)");
    }
    return getFileCountForOneNode(getRoot().getChild(nodes[1]));
  }

  private int getFileCountForOneNode(MNode node) {

    if (node.isStorageLevel()) {
      return 1;
    }
    int sum = 0;
    if (!node.isLeaf()) {
      for (MNode child : node.getChildren().values()) {
        sum += getFileCountForOneNode(child);
      }
    }
    return sum;
  }

  /**
   * Get all device type in current Metadata Tree.
   *
   * @return a list contains all distinct device type
   */
  public ArrayList<String> getAllType() {
    ArrayList<String> res = new ArrayList<>();
    if (getRoot() != null) {
      for (String type : getRoot().getChildren().keySet()) {
        res.add(type);
      }
    }
    return res;
  }

  /**
   * Get all storage groups in current Metadata Tree.
   *
   * @return a list contains all distinct storage groups
   */
  public HashSet<String> getAllStorageGroup() {
    HashSet<String> res = new HashSet<>();
    MNode root;
    if ((root = getRoot()) != null) {
      findStorageGroup(root, "root", res);
    }
    return res;
  }

  private void findStorageGroup(MNode node, String path, HashSet<String> res) {
    if (node.isStorageLevel()) {
      res.add(path);
      return;
    }
    for (MNode childNode : node.getChildren().values()) {
      findStorageGroup(childNode, path + "." + childNode.toString(), res);
    }
  }

  /**
   * Get all delta objects for given type.
   *
   * @param type device Type
   * @return a list contains all delta objects for given type
   */
  public ArrayList<String> getDeviceForOneType(String type) throws PathErrorException {
    String path = getRoot().getName() + "." + type;
    checkPath(path);
    HashMap<String, Integer> deviceMap = new HashMap<>();
    MNode typeNode = getRoot().getChild(type);
    putDeviceToMap(getRoot().getName(), typeNode, deviceMap);
    ArrayList<String> res = new ArrayList<>();
    res.addAll(deviceMap.keySet());
    return res;
  }

  private void putDeviceToMap(String path, MNode node, HashMap<String, Integer> deviceMap) {
    if (node.isLeaf()) {
      deviceMap.put(path, 1);
    } else {
      for (String child : node.getChildren().keySet()) {
        String newPath = path + "." + node.getName();
        putDeviceToMap(newPath, node.getChildren().get(child), deviceMap);
      }
    }
  }

  /**
   * Get all ColumnSchemas for given delta object type.
   *
   * @param path A seriesPath represented one Delta object
   * @return a list contains all column schema
   */
  public ArrayList<ColumnSchema> getSchemaForOneType(String path) throws PathErrorException {
    String[] nodes = path.split(separator);
    if (nodes.length != 2 || !nodes[0].equals(getRoot().getName()) || !getRoot()
        .hasChild(nodes[1])) {
      throw new PathErrorException(
          "Timeseries must be " + getRoot().getName()
              + ". X (X is one of the nodes of root's children)");
    }
    HashMap<String, ColumnSchema> leafMap = new HashMap<>();
    putLeafToLeafMap(getRoot().getChild(nodes[1]), leafMap);
    ArrayList<ColumnSchema> res = new ArrayList<>();
    res.addAll(leafMap.values());
    return res;
  }

  /**
   * Get all ColumnSchemas for the filenode seriesPath.
   *
   * @return ArrayList<  ColumnSchema  > The list of the schema
   */
  public ArrayList<ColumnSchema> getSchemaForOneFileNode(String path) {

    String[] nodes = path.split(separator);
    HashMap<String, ColumnSchema> leafMap = new HashMap<>();
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    // cur is the storage group node
    putLeafToLeafMap(cur, leafMap);
    ArrayList<ColumnSchema> res = new ArrayList<>();
    res.addAll(leafMap.values());
    return res;
  }

  /**
   * function for getting schema map for one file node.
   */
  public Map<String, ColumnSchema> getSchemaMapForOneFileNode(String path) {
    String[] nodes = path.split(separator);
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    return cur.getSchemaMap();
  }

  /**
   * function for getting num schema map for one file node.
   */
  public Map<String, Integer> getNumSchemaMapForOneFileNode(String path) {
    String[] nodes = path.split(separator);
    MNode cur = getRoot();
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
    }
    return cur.getNumSchemaMap();
  }

  private void putLeafToLeafMap(MNode node, HashMap<String, ColumnSchema> leafMap) {
    if (node.isLeaf()) {
      if (!leafMap.containsKey(node.getName())) {
        leafMap.put(node.getName(), node.getSchema());
      }
      return;
    }
    for (MNode child : node.getChildren().values()) {
      putLeafToLeafMap(child, leafMap);
    }
  }

  private void findPath(MNode node, String[] nodes, int idx, String parent,
      HashMap<String, ArrayList<String>> paths) {
    if (node.isLeaf()) {
      if (nodes.length <= idx) {
        String fileName = node.getDataFileName();
        String nodePath = parent + node;
        putAPath(paths, fileName, nodePath);
      }
      return;
    }
    String nodeReg;
    if (idx >= nodes.length) {
      nodeReg = "*";
    } else {
      nodeReg = nodes[idx];
    }

    if (!nodeReg.equals("*")) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", paths);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findPath(child, nodes, idx + 1, parent + node.getName() + ".", paths);
      }
    }
    return;
  }

  /*
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   */
  private void findPath(MNode node, String[] nodes, int idx, String parent,
      List<List<String>> res) {
    if (node.isLeaf()) {
      if (nodes.length <= idx) {
        String nodePath = parent + node;
        List<String> tsRow = new ArrayList<>(4);// get [name,storage group,dataType,encoding]
        tsRow.add(nodePath);
        ColumnSchema columnSchema = node.getSchema();
        tsRow.add(node.getDataFileName());
        tsRow.add(columnSchema.dataType.toString());
        tsRow.add(columnSchema.encoding.toString());
        res.add(tsRow);
      }
      return;
    }
    String nodeReg;
    if (idx >= nodes.length) {
      nodeReg = "*";
    } else {
      nodeReg = nodes[idx];
    }

    if (!nodeReg.equals("*")) {
      if (node.hasChild(nodeReg)) {
        findPath(node.getChild(nodeReg), nodes, idx + 1, parent + node.getName() + ".", res);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findPath(child, nodes, idx + 1, parent + node.getName() + ".", res);
      }
    }
    return;
  }

  private void putAPath(HashMap<String, ArrayList<String>> paths, String fileName,
      String nodePath) {
    if (paths.containsKey(fileName)) {
      paths.get(fileName).add(nodePath);
    } else {
      ArrayList<String> pathList = new ArrayList<>();
      pathList.add(nodePath);
      paths.put(fileName, pathList);
    }
  }

  public String toString() {
    return mnodeToString(getRoot(), 0);
  }

  private String mnodeToString(MNode node, int tab) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < tab; i++) {
      builder.append(space);
    }
    builder.append(node.getName());
    if (!node.isLeaf() && node.getChildren().size() > 0) {
      builder.append(":{\n");
      int first = 0;
      for (MNode child : node.getChildren().values()) {
        if (first == 0) {
          first = 1;
        } else {
          builder.append(",\n");
        }
        builder.append(mnodeToString(child, tab + 1));
      }
      builder.append("\n");
      for (int i = 0; i < tab; i++) {
        builder.append(space);
      }
      builder.append("}");
    } else if (node.isLeaf()) {
      builder.append(":{\n");
      builder
          .append(String.format("%s DataType: %s,\n", getTabs(tab + 1), node.getSchema().dataType));
      builder
          .append(String.format("%s Encoding: %s,\n", getTabs(tab + 1), node.getSchema().encoding));
      builder
          .append(String.format("%s args: %s,\n", getTabs(tab + 1), node.getSchema().getArgsMap()));
      builder.append(
          String.format("%s StorageGroup: %s \n", getTabs(tab + 1), node.getDataFileName()));
      builder.append(getTabs(tab));
      builder.append("}");
    }
    return builder.toString();
  }

  private String getTabs(int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(space);
    }
    return sb.toString();
  }

  public MNode getRoot() {
    return root;
  }
}
